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

	// Get current ballot number
	currentBallotNumber := s.state.GetBallotNumber()

	// Forward request if not leader
	if !s.state.IsLeader() {
		if s.state.InForwardedRequestsLog(req) {
			log.Warnf("[TransferRequest] Request %s already forwarded", utils.TransactionRequestString(req))
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "not leader")
		}
		sequenceNum := s.state.StateLog.GetSequenceNumber(req)
		if s.state.StateLog.IsAccepted(sequenceNum) && proto.Equal(s.state.StateLog.GetBallotNumber(sequenceNum), currentBallotNumber) {
			log.Warnf("[TransferRequest] Request %s already accepted", utils.TransactionRequestString(req))
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "already accepted")
		}
		// Logger: Add received transaction request
		s.logger.AddReceivedTransactionRequest(req)

		// Add request to forwarded requests log
		s.state.AddForwardedRequest(req)

		s.acceptor.timer.IncrementWaitCountOrStart()
		leader := s.state.GetLeader()
		if leader != "" {
			go s.ForwardToLeader(leader, req)
		}
		return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "not leader")
	}

	// Logger: Add received transaction request
	s.logger.AddReceivedTransactionRequest(req)

	// Duplicate requests with same timestamp are not ignored since the reply could have been lost
	timestamp, result := s.state.DedupTable.GetLastResult(req.Sender)
	if timestamp != 0 && req.Timestamp == timestamp {
		response := &pb.TransactionResponse{
			B:         currentBallotNumber,
			Timestamp: req.Timestamp,
			Sender:    req.Sender,
			Result:    utils.Int64ToBool(result),
		}
		// Logger: Add sent transaction response
		s.logger.AddSentTransactionResponse(response)
		return response, nil
	}

	// Older timestamp requests are ignored
	if timestamp != 0 && req.Timestamp < timestamp {
		log.Warnf("[TransferRequest] Ignored %s; Last reply timestamp %d", utils.TransactionRequestString(req), timestamp)
		return UnsuccessfulTransactionResponse, status.Errorf(codes.AlreadyExists, "old timestamp")
	}

	// Handle transaction request
	err := s.proposer.HandleTransactionRequest(req)
	if err != nil {
		return UnsuccessfulTransactionResponse, status.Error(codes.Aborted, err.Error())
	}

	// Create transaction response from dedup table
	timestamp, result = s.state.DedupTable.GetLastResult(req.Sender)
	response := &pb.TransactionResponse{
		B:         currentBallotNumber,
		Timestamp: timestamp,
		Sender:    req.Sender,
		Result:    utils.Int64ToBool(result),
	}

	// Logger: Add sent transaction response
	s.logger.AddSentTransactionResponse(response)

	return response, nil
}

// ForwardToLeader forwards the request to the leader
func (s *PaxosServer) ForwardToLeader(leader string, req *pb.TransactionRequest) {
	// Logger: Add forwarded transaction request
	s.logger.AddForwardedTransactionRequest(req)

	// Forward request to leader
	log.Infof("[ForwardToLeader] Forwarding request %s to leader %s", utils.TransactionRequestString(req), leader)
	(*s.peers[leader].Client).ForwardRequest(context.Background(), req)
}
