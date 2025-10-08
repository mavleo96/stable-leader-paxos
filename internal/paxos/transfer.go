package paxos

import (
	"context"

	"github.com/mavleo96/cft-mavleo96/internal/utils"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func (s *PaxosServer) TransferRequest(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	// Forward request if not leader; acquire read lock on state mutex
	s.State.Mutex.RLock()
	if s.State.Leader.ID != s.NodeID {
		s.State.Mutex.RUnlock() // Release read lock on state mutex
		s.PaxosTimer.IncrementWaitCountOrStart()
		responseCh := make(chan error)
		log.Warnf("Not leader, forwarding to leader %s", s.State.Leader)
		go func(r *pb.TransactionRequest) {
			err := s.ForwardToLeader(r)
			responseCh <- err
		}(req)

		// TODO: currently waiting for leader to respond before returning to client
		// Maybe need to return a response to client immediately and forward in separate goroutine
		select {
		case <-s.PaxosTimer.Timer.C:
			log.Warnf("Leader timer expired, need to initiate prepare")
			// TODO: ensure cleanup of routines waiting and timer
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Unavailable, "leader timed out")
		case err := <-responseCh:
			if err != nil {
				// TODO: ensure cleanup of routines waiting and timer
				// If error is not due to leader not being available, return failed forward
				return UnsuccessfulTransactionResponse, err
			}
			s.PaxosTimer.DecrementWaitCountAndResetOrStopIfZero()
			// If successful, return unsuccessful transaction response since this is not the leader
			log.Infof("Successfully forwarded to leader %s: %s", s.State.Leader, utils.TransactionRequestString(req))
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Unavailable, "not leader")
		}
	}

	// Release the read lock and acquire write lock since we need to process the request
	s.State.Mutex.RUnlock()
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()

	// We will process even if the timestamp is equal because previous reply could have been lost or sent to backup node
	if req.Timestamp < s.LastReplyTimestamp[req.Sender] {
		log.Warnf("Old timestamp %d < %d", req.Timestamp, s.LastReplyTimestamp[req.Sender])
		return UnsuccessfulTransactionResponse, status.Errorf(codes.AlreadyExists, "old timestamp")
	}

	// ACCEPT REQUEST LOGIC
	// Check if the request is already accepted
	sequenceNum, _ := s.CheckIfRequestInAcceptLog(req)
	// If new request then assign a higher sequence number and accept it immediately before sending accepts
	if sequenceNum == 0 {
		s.CurrentSequenceNum++
		sequenceNum = s.CurrentSequenceNum
		log.Infof("Accepted %s", utils.TransactionRequestString(req))
		s.State.AcceptLog[sequenceNum] = &pb.AcceptRecord{
			AcceptedBallotNumber:   s.CurrentBallotNum,
			AcceptedSequenceNumber: sequenceNum,
			AcceptedVal:            req,
			Committed:              false,
			Executed:               false,
			Result:                 false,
		}
	}

	// COMMIT REQUEST LOGIC
	// Check if the request is already committed
	if !s.State.AcceptLog[sequenceNum].Committed {
		ok, err := s.SendAcceptRequest(&pb.AcceptMessage{
			B:           s.CurrentBallotNum,
			SequenceNum: sequenceNum,
			Message:     req,
		})
		if err != nil {
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Internal, err.Error())
		}
		if !ok {
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Internal, "failed to get quorum of accepts")
		}

		// Once quorum of accepts in recieved commit it immediately and multicast the commit request
		s.State.AcceptLog[sequenceNum].Committed = true
		// TODO: no error handling is prolly bad; but the function always returns nil
		s.SendCommitRequest(&pb.CommitMessage{
			B:           s.CurrentBallotNum,
			SequenceNum: sequenceNum,
			Transaction: req,
		})
	}

	// EXECUTE REQUEST LOGIC
	// Try to execute the request and return the result
	result, err := s.TryExecute(sequenceNum)
	if err != nil {
		return UnsuccessfulTransactionResponse, status.Errorf(codes.Internal, "could not commit request WITH %v", err)
	}
	s.LastReplyTimestamp[req.Sender] = req.Timestamp
	return &pb.TransactionResponse{
		B:         s.CurrentBallotNum,
		Timestamp: req.Timestamp,
		Sender:    req.Sender,
		Result:    result,
	}, nil
}

// ForwardToLeader forwards the request to the leader
func (s *PaxosServer) ForwardToLeader(req *pb.TransactionRequest) error {
	// Initialize connection to leader
	conn, err := grpc.NewClient(s.State.Leader.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Warnf("Failed to forward to leader %s: %s", s.State.Leader, err)
		return status.Errorf(codes.Internal, "failed to forward to leader")
	}
	defer conn.Close()
	leaderClient := pb.NewPaxosClient(conn)

	// Forward request to leader
	_, err = leaderClient.TransferRequest(context.Background(), req)
	return err
}

// TODO: This should be a util function
func (s *PaxosServer) CheckIfRequestInAcceptLog(req *pb.TransactionRequest) (int64, error) {
	// TODO: This is a development hack; need to remove this
	if s.State.Mutex.TryLock() {
		log.Fatal("State mutex was not acquired before trying to execute!")
	}

	for _, acceptRecord := range s.State.AcceptLog {
		acceptedVal := acceptRecord.AcceptedVal
		if acceptedVal.Sender == req.Sender && acceptedVal.Timestamp == req.Timestamp {
			return acceptRecord.AcceptedSequenceNumber, nil
		}
	}
	return int64(0), nil
}
