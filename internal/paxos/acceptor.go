package paxos

import (
	"context"
	"errors"

	"github.com/mavleo96/cft-mavleo96/internal/utils"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
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
		return &pb.AcceptedMessage{Ok: false, B: req.B, SequenceNum: req.SequenceNum, Message: req.Message, AcceptorID: s.NodeID}, nil
	}

	// Update State
	// Replace if sequence number exists in accept log else append
	// Update leader since this is a accept request with higher ballot number
	s.State.Leader = s.Peers[req.B.NodeID]
	s.State.PromisedBallotNum = req.B // This is wrong according to Prajwal
	seqNum, ok := GetSequenceNumberIfExistsInAcceptLog(s.State.AcceptLog, req.Message)
	if ok {
		s.State.AcceptLog[seqNum].AcceptedBallotNumber = req.B
		log.Fatal("Sequence number already exists in accept log") // TODO: this is a development hack; need to fix this
	} else {
		s.State.AcceptLog[req.SequenceNum] = &pb.AcceptRecord{
			AcceptedBallotNumber:   req.B,
			AcceptedSequenceNumber: req.SequenceNum,
			AcceptedVal:            req.Message,
			Committed:              false,
			Executed:               false,
		}
		// We increment wait count since this is a new request
		s.PaxosTimer.IncrementWaitCountOrStart(req.String())
	}

	log.Infof("Accepted %s", utils.TransactionRequestString(req.Message))
	return &pb.AcceptedMessage{
		Ok:          true,
		B:           req.B,
		SequenceNum: req.SequenceNum,
		Message:     req.Message,
		AcceptorID:  s.NodeID,
	}, nil
}

// CommitRequest handles the commit request rpc on server side
// This code is part of Acceptor structure
func (s *PaxosServer) CommitRequest(ctx context.Context, req *pb.CommitMessage) (*emptypb.Empty, error) {
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()

	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.State.PromisedBallotNum, req.B) {
		log.Warnf("Rejected %s", utils.TransactionRequestString(req.Transaction))
		return &emptypb.Empty{}, nil
	}

	// If not already in log then increment wait count
	_, ok := s.State.AcceptLog[req.SequenceNum]
	if !ok {
		s.PaxosTimer.IncrementWaitCountOrStart(req.String())
	}

	// Update State
	// Update leader since this is a accept request with higher ballot number
	s.State.Leader = s.Peers[req.B.NodeID]
	s.State.PromisedBallotNum = req.B // This is wrong according to Prajwal

	// Replace if sequence number exists in accept log else append
	seqNum, ok := GetSequenceNumberIfExistsInAcceptLog(s.State.AcceptLog, req.Transaction)
	if ok {
		s.State.AcceptLog[seqNum].AcceptedBallotNumber = req.B
		s.State.AcceptLog[seqNum].Committed = true
	} else {
		s.State.AcceptLog[req.SequenceNum] = &pb.AcceptRecord{
			AcceptedBallotNumber:   req.B,
			AcceptedSequenceNumber: req.SequenceNum,
			AcceptedVal:            req.Transaction,
			Committed:              true,
			Executed:               false,
		}
	}
	log.Infof("Commited %s", utils.TransactionRequestString(req.Transaction))

	// Try to execute the request
	// TODO: this busy retry logic can be problematic; need to fix this
	for {
		_, err := s.TryExecute(req.SequenceNum)
		if err != nil {
			log.Infof("Retry execute request %s", utils.TransactionRequestString(req.Transaction))
			continue
		}
		// Decrement wait count since request is executed
		s.PaxosTimer.DecrementWaitCountAndResetOrStopIfZero(req.String())
		break
	}
	return &emptypb.Empty{}, nil
}

// TryExecute tries to execute the transaction
// The state mutex should be acquired before calling this function
func (s *PaxosServer) TryExecute(sequenceNum int64) (bool, error) {
	// TODO: This is a development hack; need to remove this
	if s.State.Mutex.TryLock() {
		log.Fatal("State mutex was not acquired before trying to execute!")
	}

	// Execute transactions until the sequence number is executed
	for s.State.ExecutedSequenceNum < sequenceNum {
		nextSequenceNum := s.State.ExecutedSequenceNum + 1
		record, ok := s.State.AcceptLog[nextSequenceNum]
		if !ok || !record.Committed {
			return false, errors.New("not executed since log has gaps")
		}
		if record.Executed {
			log.Fatal("Executed sequence number is already executed")
		}
		success, err := s.DB.UpdateDB(record.AcceptedVal.Transaction)
		if err != nil {
			log.Warn(err)
		}
		record.Executed = true
		record.Result = success
		log.Infof("Executed %s", utils.TransactionRequestString(record.AcceptedVal))
		s.State.ExecutedSequenceNum++
	}
	return s.State.AcceptLog[sequenceNum].Result, nil
}
