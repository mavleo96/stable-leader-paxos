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

	// Ignore if timer context is cancelled
	select {
	case <-s.phaseManager.GetTimerCtx().Done():
		log.Warnf("[TransferRequest] Timer context cancelled; ignoring request %s", utils.LoggingString(req))
		return EmptyTransactionResponse, status.Errorf(codes.Aborted, "timer context cancelled")
	default:
	}

	// // Get current ballot number
	currentBallotNumber := s.state.GetBallotNumber()
	log.Infof("[TransferRequest] Request %s to be processed with current ballot number: %s", utils.LoggingString(req), utils.LoggingString(currentBallotNumber))

	// Forward request if not leader
	if !s.state.IsLeader() {
		// If request is already forwarded, return empty transaction response
		if s.state.InForwardedRequestsLog(req) {
			log.Warnf("[TransferRequest] Request %s already forwarded for ballot number: %s", utils.LoggingString(req), utils.LoggingString(currentBallotNumber))
			return EmptyTransactionResponse, status.Errorf(codes.Aborted, "not leader; already forwarded")
		}

		// If request is already accepted in current ballot number, return empty transaction response
		sequenceNum := s.state.StateLog.GetSequenceNumber(req)
		if s.state.StateLog.IsAccepted(sequenceNum) && proto.Equal(s.state.StateLog.GetBallotNumber(sequenceNum), currentBallotNumber) {
			log.Warnf("[TransferRequest] Request %s already accepted", utils.LoggingString(req))
			return EmptyTransactionResponse, status.Errorf(codes.Aborted, "not leader; already accepted")
		}

		// Logger: Add received transaction request
		s.logger.AddReceivedTransactionRequest(req)

		// Add request to forwarded requests log and start timer
		s.state.AddForwardedRequest(req)
		s.phaseManager.timer.IncrementWaitCountOrStart()

		// Forward request to leader
		leader := s.state.GetLeader()
		if leader != "" {
			go s.ForwardToLeader(leader, req)
		}
		return EmptyTransactionResponse, status.Errorf(codes.Aborted, "not leader; forwarded to leader")
	}

	// Leader logic

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
		log.Warnf("[TransferRequest] Ignored %s; Last reply timestamp %d", utils.LoggingString(req), timestamp)
		return EmptyTransactionResponse, status.Errorf(codes.AlreadyExists, "old timestamp")
	}

	// Handle transaction request
	err := s.proposer.HandleTransactionRequest(req)
	if err != nil {
		return EmptyTransactionResponse, status.Error(codes.FailedPrecondition, err.Error())
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
	log.Infof("[ForwardToLeader] Forwarding request %s to leader %s", utils.LoggingString(req), leader)
	(*s.peers[leader].Client).ForwardRequest(context.Background(), req)
}
