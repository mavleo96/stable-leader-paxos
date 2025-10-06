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
	// s.Mutex.RLock()
	// defer s.Mutex.RUnlock()
	if s.State.Leader.ID != s.NodeID {
		s.PaxosTimer.IncrementWaitCountOrStart()

		responseCh := make(chan error)
		log.Warnf("Not leader, forwarding to leader %s", s.State.Leader)
		go func(r *pb.TransactionRequest) {
			err := s.ForwardToLeader(r)
			responseCh <- err
		}(req)

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

	// TODO: think if this should be at the beginning of the function
	// We will process even if the timestamp is equal because previous reply could have been lost or sent to backup node
	if req.Timestamp < s.LastReplyTimestamp[req.Sender] {
		return UnsuccessfulTransactionResponse, status.Errorf(codes.AlreadyExists, "old timestamp")
	}
	// Check if the request is already accepted or assign a higher sequence number
	s.State.Mutex.RLock()
	for _, acceptRecord := range s.State.AcceptLog {
		acceptedVal := acceptRecord.AcceptedVal
		if acceptedVal.Sender == req.Sender && acceptedVal.Timestamp == req.Timestamp {
			// TODO: Need to think about this; what if its not committed, or not executed
			if !acceptRecord.Executed {
				s.State.Mutex.RUnlock()
				return UnsuccessfulTransactionResponse, status.Errorf(codes.AlreadyExists, "commmited but not executed")
			} else {
				s.State.Mutex.RUnlock()
				return &pb.TransactionResponse{
					B:         s.CurrentBallotNum,
					Timestamp: req.Timestamp,
					Sender:    req.Sender,
					Result:    acceptRecord.Result,
				}, nil
			}
		}
	}
	s.State.Mutex.RUnlock()

	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.CurrentSequenceNum++
	assignSequenceNum := s.CurrentSequenceNum

	ok, err := s.SendAcceptRequest(&pb.AcceptMessage{
		B:           s.CurrentBallotNum,
		SequenceNum: assignSequenceNum,
		Message:     req,
	})
	if err != nil || !ok {
		return UnsuccessfulTransactionResponse, status.Errorf(codes.Internal, "failed to send accept request")
	}
	// TODO: no error handling is prolly bad; but the function always returns nil
	resp, err := s.SendCommitRequest(&pb.CommitMessage{
		B:           s.CurrentBallotNum,
		SequenceNum: assignSequenceNum,
		Transaction: req,
	})
	if err != nil {
		if !resp.Committed {
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Internal, "could not commit request")
		}
		if !resp.Executed {
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Internal, "could not execute request")
		}
		// TODO: improve this code flow
		log.Fatalf("should not happen")
	}

	s.LastReplyTimestamp[req.Sender] = req.Timestamp
	return &pb.TransactionResponse{
		B:         s.CurrentBallotNum,
		Timestamp: req.Timestamp,
		Sender:    req.Sender,
		Result:    resp.Result,
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
