package paxos

import (
	"context"

	"github.com/mavleo96/cft-mavleo96/internal/utils"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
)

// All functions are part of Acceptor structure
// They should only try to acquire the state mutex

// func (s *PaxosServer) PrepareRequest(ctx context.Context, req *pb.PrepareMessage) (*pb.AckMessage, error) {
// 	// TODO: think what if leader node needs to handle this
// 	s.State.Mutex.Lock()
// 	defer s.State.Mutex.Unlock()

// 	// TODO: don't promise if timer has not expired -> need to log messages and then respond to highest
// 	if !BallotNumberIsHigher(s.State.PromisedBallotNum, req.B) {
// 		log.Warnf("Rejected prepare request %v", req.String())
// 		return &pb.AckMessage{Ok: false}, nil
// 	}

// 	s.State.PromisedBallotNum = req.B
// 	log.Infof("Accepted prepare request %v", req.String())
// 	return &pb.AckMessage{
// 		Ok:        true,
// 		AcceptNum: s.State.PromisedBallotNum,
// 		AcceptLog: s.State.AcceptLog,
// 	}, nil
// }

// AcceptRequest handles the accept request rpc on server side
// This code is part of Acceptor structure
func (s *PaxosServer) AcceptRequest(ctx context.Context, req *pb.AcceptMessage) (*pb.AcceptedMessage, error) {
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()

	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.State.PromisedBallotNum, req.B) {
		log.Warnf("Rejected %s", utils.TransactionRequestString(req.Message))
		return &pb.AcceptedMessage{Ok: false, SequenceNum: req.SequenceNum, AcceptorID: s.NodeID}, nil
	}

	// Update State
	// Replace if sequence number exists in accept log else append
	// Update leader since this is a accept request with higher ballot number
	s.State.Leader = req.B.NodeID
	// s.State.PromisedBallotNum = req.B // This is wrong according to Prajwal
	s.State.AcceptLog[req.SequenceNum] = &pb.AcceptRecord{
		AcceptedBallotNumber:   req.B,
		AcceptedSequenceNumber: req.SequenceNum,
		AcceptedVal:            req.Message,
		Committed:              false,
		Executed:               false,
	}

	log.Infof("Accepted %s", utils.TransactionRequestString(req.Message))
	return &pb.AcceptedMessage{
		Ok:          true,
		SequenceNum: req.SequenceNum,
		Message:     req.Message,
		AcceptorID:  s.NodeID,
	}, nil
}

// CommitRequest handles the commit request rpc on server side
// This code is part of Acceptor structure
func (s *PaxosServer) CommitRequest(ctx context.Context, req *pb.CommitMessage) (*pb.CommitResponse, error) {
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()

	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.State.PromisedBallotNum, req.B) {
		log.Warnf("Rejected %s", utils.TransactionRequestString(req.Transaction))
		return &pb.CommitResponse{Success: false}, nil
	}

	// Update State
	// Replace if sequence number exists in accept log else append
	// Update leader since this is a accept request with higher ballot number
	s.State.Leader = req.B.NodeID
	s.State.AcceptLog[req.SequenceNum] = &pb.AcceptRecord{
		AcceptedBallotNumber:   req.B,
		AcceptedSequenceNumber: req.SequenceNum,
		AcceptedVal:            req.Transaction,
		Committed:              true,
		Executed:               false,
	}
	log.Infof("Commited %s", utils.TransactionRequestString(req.Transaction))

	// Execute as much as possible
	success := s.TryExecute(req.SequenceNum)
	if !success {
		log.Warnf("Failed to execute commit request %s", utils.TransactionRequestString(req.Transaction))
		return &pb.CommitResponse{Success: false}, nil
	}

	return &pb.CommitResponse{Success: true}, nil
}

// TryExecute tries to execute the transaction
// The state mutex should be acquired before calling this function
func (s *PaxosServer) TryExecute(sequenceNum int64) bool {
	for s.State.ExecutedSequenceNum < sequenceNum {
		nextSequenceNum := s.State.ExecutedSequenceNum + 1
		record, ok := s.State.AcceptLog[nextSequenceNum]
		if !ok {
			return false
		}
		if !record.Committed {
			return false
		}
		if record.Executed {
			log.Fatal("Executed sequence number is already executed")
		}
		success, err := s.DB.UpdateDB(record.AcceptedVal.Transaction)
		if err != nil {
			log.Warn(err)
		}
		record.Executed = true
		log.Infof("Executed %s with success %t", utils.TransactionRequestString(record.AcceptedVal), success)
		s.State.ExecutedSequenceNum++
	}
	return true
}
