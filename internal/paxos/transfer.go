package paxos

import (
	"context"

	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *PaxosServer) TransferRequest(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	ctxMain, cancel := context.WithTimeout(context.Background(), mainTimeout)
	defer cancel()

	// s.Mutex.RLock()
	// defer s.Mutex.RUnlock()
	//
	if s.State.Leader != s.NodeID {
		// TODO: forward to leader with mainTimeout context
		// goroutine with response channel
		responseCh := make(chan bool)
		go func(ch chan bool) {
			ch <- false
		}(responseCh)
		select {
		case <-responseCh:
			log.Warnf("Not leader, forwarding to leader %s", s.State.Leader)
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Unavailable, "not leader")
		case <-ctxMain.Done():
			// Need to wait for prepare or initiate prepare
		}
	}

	// TODO: think if this should be at the beginning of the function
	if req.Timestamp <= s.LastReplyTimestamp[req.Sender] {
		return UnsuccessfulTransactionResponse, status.Errorf(codes.AlreadyExists, "timestamp already exists")
	}
	// Check if the request is already accepted or assign a higher sequence number
	s.State.Mutex.RLock()
	for _, acceptRecord := range s.State.AcceptLog {
		acceptedVal := acceptRecord.AcceptedVal
		if acceptedVal.Sender == req.Sender && acceptedVal.Timestamp == req.Timestamp {
			return UnsuccessfulTransactionResponse, status.Errorf(codes.AlreadyExists, "already accepted")
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
