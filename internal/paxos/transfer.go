package paxos

import (
	"context"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TransferRequest is the main function that rpc server calls to handle the transaction request
func (s *PaxosServer) TransferRequest(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	if !s.config.Alive {
		log.Warnf("Node %s is not alive", s.ID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Forward request if not leader
	if s.state.GetLeader() != s.ID {
		if s.state.InForwardedRequestsLog(req) {
			log.Warnf("[TransferRequest] Request %s already forwarded", utils.TransactionRequestString(req))
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "not leader")
		}
		sequenceNum := s.state.StateLog.GetSequenceNumber(req)
		if s.state.StateLog.IsAccepted(sequenceNum) && proto.Equal(s.state.StateLog.GetBallotNumber(sequenceNum), s.state.GetBallotNumber()) {
			log.Warnf("[TransferRequest] Request %s already accepted", utils.TransactionRequestString(req))
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "already accepted")
		}
		// Logger: Add received transaction request
		s.logger.AddReceivedTransactionRequest(req)

		s.acceptor.timer.IncrementWaitCountOrStart()
		if s.state.GetLeader() != "" {
			go s.ForwardToLeader(req)
		}
		s.state.AddForwardedRequest(req)
		return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "not leader")
	}

	// Logger: Add received transaction request
	s.logger.AddReceivedTransactionRequest(req)

	// Duplicate requests with same timestamp are not ignored since the reply could have been lost
	lastReply := s.state.LastReply.Get(req.Sender)
	if lastReply != nil && req.Timestamp == lastReply.Timestamp {
		return s.state.LastReply.Get(req.Sender), nil
	}
	// Older timestamp requests are ignored
	if lastReply != nil && req.Timestamp < lastReply.Timestamp {
		log.Warnf("Ignored %s; Last reply timestamp %d", utils.TransactionRequestString(req), s.state.LastReply.Get(req.Sender).Timestamp)
		return UnsuccessfulTransactionResponse, status.Errorf(codes.AlreadyExists, "old timestamp")
	}

	sequenceNum := s.proposer.HandleTransactionRequest(req)

	// Add response channel for sequence number
	responseCh := make(chan int64)
	s.executor.AddResponseChannel(sequenceNum, responseCh)

	// Run protocol
	go s.proposer.RunProtocol(sequenceNum)

	// Wait for response from executor
	result := <-responseCh
	if result == -1 {
		return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "execute request failed try again")
	}

	response := &pb.TransactionResponse{
		B:         s.state.GetBallotNumber(),
		Timestamp: req.Timestamp,
		Sender:    req.Sender,
		Result:    utils.Int64ToBool(result),
	}
	s.state.LastReply.Update(req.Sender, response)
	return response, nil
}

// ForwardToLeader forwards the request to the leader
func (s *PaxosServer) ForwardToLeader(req *pb.TransactionRequest) {
	// Logger: Add forwarded transaction request
	s.logger.AddForwardedTransactionRequest(req)

	// Forward request to leader
	log.Infof("[ForwardToLeader] Forwarding request %s to leader %s", utils.TransactionRequestString(req), s.state.GetLeader())
	(*s.peers[s.state.GetLeader()].Client).ForwardRequest(context.Background(), req)
}
