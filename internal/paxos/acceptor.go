package paxos

import (
	"context"
	"errors"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// All functions are part of Acceptor structure
// They should only try to acquire the state mutex

// PrepareRequest handles the prepare request rpc on server side
func (s *PaxosServer) PrepareRequest(ctx context.Context, req *pb.PrepareMessage) (*pb.AckMessage, error) {
	s.State.Mutex.RLock()
	if !s.IsAlive {
		s.State.Mutex.RUnlock()
		log.Warnf("Node is dead")
		return &pb.AckMessage{Ok: false}, status.Errorf(codes.Unavailable, "node is dead")
	}

	log.Infof("Received prepare request %v", utils.BallotNumberString(req.B))

	// Reject immediately if ballot number is lower or equal to promise
	if !BallotNumberIsHigher(s.State.PromisedBallotNum, req.B) {
		s.State.Mutex.RUnlock()
		log.Warnf("Rejected prepare request %s without queueing", utils.BallotNumberString(req.B))
		return &pb.AckMessage{Ok: false}, nil
	}
	s.State.Mutex.RUnlock()

	// Add prepare request record to prepare message log
	prepareRequestRecord := &PrepareRequestRecord{
		ResponseChannel: make(chan *pb.PrepareMessage),
		PrepareMessage:  req,
	}
	s.PrepareMessageLog[time.Now()] = prepareRequestRecord
	s.State.ReceivedPrepareMessages = append(s.State.ReceivedPrepareMessages, req)

	if !s.SysInitialized {
		log.Infof("System not initialized, immediately accepting prepare request %s", utils.BallotNumberString(req.B))
		s.SysInitialized = true
		s.State.PromisedBallotNum = req.B
		close(prepareRequestRecord.ResponseChannel)
		prepareRequestRecord.ResponseChannel = nil
		s.State.Leader = &models.Node{
			ID:      s.NodeID,
			Address: s.Addr,
		}
		return &pb.AckMessage{
			Ok:        true,
			AcceptNum: req.B,
			AcceptLog: make([]*pb.AcceptRecord, 0),
		}, nil
	}

	// Wait for prepare routine to send prepare message
	if resp := <-prepareRequestRecord.ResponseChannel; resp != prepareRequestRecord.PrepareMessage {
		log.Infof("Rejected prepare request %s", utils.BallotNumberString(req.B))
		return &pb.AckMessage{Ok: false}, nil
	}

	// Prepare the log to be sent
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	acceptLog := make([]*pb.AcceptRecord, 0)
	for _, record := range s.State.AcceptLog {
		acceptLog = append(acceptLog, record)
	}

	log.Infof("Accepted prepare request %s", utils.BallotNumberString(req.B))
	return &pb.AckMessage{
		Ok:        true,
		AcceptNum: req.B,
		AcceptLog: acceptLog,
	}, nil
}

// NewViewRequest handles the new view request rpc on server side
func (s *PaxosServer) NewViewRequest(req *pb.NewViewMessage, stream pb.Paxos_NewViewRequestServer) error {
	s.State.Mutex.RLock()
	if !s.IsAlive {
		s.State.Mutex.RUnlock()
		log.Warnf("Node is dead")
		return status.Errorf(codes.Unavailable, "node is dead")
	}
	s.State.Mutex.RUnlock()

	// No need to acquire state mutex since AcceptRequest acquires it
	log.Infof("Received new view message %v", utils.BallotNumberString(req.B))
	s.State.Mutex.Lock()
	s.State.NewViewLog = append(s.State.NewViewLog, req)
	s.State.Mutex.Unlock()

	for _, record := range req.AcceptLog {
		acceptedMessage, err := s.AcceptRequest(context.Background(), &pb.AcceptMessage{
			B:           record.AcceptedBallotNum,
			SequenceNum: record.AcceptedSequenceNum,
			Message:     record.AcceptedVal,
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := stream.Send(acceptedMessage); err != nil {
			log.Fatal(err)
		}
	}
	log.Infof("Finished streaming accepts for %s", utils.BallotNumberString(req.B))
	return nil
}

// AcceptRequest handles the accept request rpc on server side
// This code is part of Acceptor structure
func (s *PaxosServer) AcceptRequest(ctx context.Context, req *pb.AcceptMessage) (*pb.AcceptedMessage, error) {
	s.State.Mutex.RLock()
	if !s.IsAlive {
		s.State.Mutex.RUnlock()
		log.Warnf("Node is dead")
		return nil, status.Errorf(codes.Unavailable, "node is dead")
	}
	s.State.Mutex.RUnlock()

	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()

	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.State.PromisedBallotNum, req.B) {
		log.Warnf("Rejected %s", utils.TransactionRequestString(req.Message))
		return &pb.AcceptedMessage{Ok: false}, nil
	}

	// Update State
	// Replace if sequence number exists in accept log else append
	// Update leader since this is a accept request with higher ballot number
	s.State.Leader = s.Peers[req.B.NodeID]
	s.State.PromisedBallotNum = req.B // This is wrong according to Prajwal
	seqNum, ok := GetSequenceNumberIfExistsInAcceptLog(s.State.AcceptLog, req.Message)
	if ok {
		s.State.AcceptLog[seqNum].AcceptedBallotNum = req.B
	} else {
		s.State.AcceptLog[req.SequenceNum] = &pb.AcceptRecord{
			AcceptedBallotNum:   req.B,
			AcceptedSequenceNum: req.SequenceNum,
			AcceptedVal:         req.Message,
			Committed:           false,
			Executed:            false,
		}
		// We increment wait count since this is a new request
		s.PaxosTimer.IncrementWaitCountOrStart()
	}

	s.State.ReceivedAcceptMessages = append(s.State.ReceivedAcceptMessages, req)

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
	s.State.Mutex.RLock()
	if !s.IsAlive {
		s.State.Mutex.RUnlock()
		log.Warnf("Node is dead")
		return nil, status.Errorf(codes.Unavailable, "node is dead")
	}
	s.State.Mutex.RUnlock()

	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()

	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.State.PromisedBallotNum, req.B) {
		log.Warnf("Rejected %s", utils.TransactionRequestString(req.Transaction))
		return &emptypb.Empty{}, nil
	}

	s.State.ReceivedCommitMessages = append(s.State.ReceivedCommitMessages, req)

	// If not already in log then increment wait count
	_, ok := s.State.AcceptLog[req.SequenceNum]
	if !ok {
		s.PaxosTimer.IncrementWaitCountOrStart()
	}

	// Update State
	// Update leader since this is a accept request with higher ballot number
	s.State.Leader = s.Peers[req.B.NodeID]
	s.State.PromisedBallotNum = req.B // This is wrong according to Prajwal

	// Replace if sequence number exists in accept log else append
	seqNum, ok := GetSequenceNumberIfExistsInAcceptLog(s.State.AcceptLog, req.Transaction)
	if ok {
		s.State.AcceptLog[seqNum].AcceptedBallotNum = req.B
		s.State.AcceptLog[seqNum].Committed = true
	} else {
		s.State.AcceptLog[req.SequenceNum] = &pb.AcceptRecord{
			AcceptedBallotNum:   req.B,
			AcceptedSequenceNum: req.SequenceNum,
			AcceptedVal:         req.Transaction,
			Committed:           true,
			Executed:            false,
		}
	}
	log.Infof("Commited %s", utils.TransactionRequestString(req.Transaction))

	// Try to execute the request
	if s.State.AcceptLog[req.SequenceNum].Executed {
		log.Infof("Request %s already executed", utils.TransactionRequestString(req.Transaction))
		return &emptypb.Empty{}, nil
	}

	_, err := s.TryExecute(req.SequenceNum)
	if err != nil {
		log.Infof("Failed to execute request %s", utils.TransactionRequestString(req.Transaction))
		return &emptypb.Empty{}, nil
	}
	s.PaxosTimer.DecrementWaitCountAndResetOrStopIfZero()
	return &emptypb.Empty{}, nil
}

// CatchupRequest handles the catchup request rpc on server side
// This code is part of Acceptor structure
func (s *PaxosServer) CatchupRequest(ctx context.Context, req *wrapperspb.Int64Value) (*pb.CatchupMessage, error) {
	// s.State.Mutex.Lock()
	// defer s.State.Mutex.Unlock()

	if !s.IsAlive {
		log.Warnf("Node is dead")
		return nil, status.Errorf(codes.Unavailable, "node is dead")
	}

	if s.State.Leader.ID != s.NodeID {
		return nil, status.Errorf(codes.Aborted, "not leader")
	}

	log.Infof("Received catch up request logs with sequence number >%d", req.Value)
	catchupLog := []*pb.AcceptRecord{}
	maxSequenceNum := MaxSequenceNumber(s.State.AcceptLog)

	for sequenceNum := req.Value + 1; sequenceNum <= maxSequenceNum; sequenceNum++ {
		record, ok := s.State.AcceptLog[sequenceNum]
		if !ok || !record.Committed {
			continue
		}
		catchupLog = append(catchupLog, record)
	}

	log.Infof("Catchup sent %d records", len(catchupLog))
	return &pb.CatchupMessage{
		B:         s.State.PromisedBallotNum,
		AcceptLog: catchupLog,
	}, nil
}

// TryExecute tries to execute the transaction
// The state mutex should be acquired before calling this function
func (s *PaxosServer) TryExecute(sequenceNum int64) (bool, error) {
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
		// Update database
		var success bool = true
		if record.AcceptedVal != NoOperation {
			var err error
			success, err = s.DB.UpdateDB(record.AcceptedVal.Transaction)
			if err != nil {
				log.Warn(err)
			}
		}
		record.Executed = true
		record.Result = success
		log.Infof("Executed %s", utils.TransactionRequestString(record.AcceptedVal))
		s.State.ExecutedSequenceNum++
	}
	return s.State.AcceptLog[sequenceNum].Result, nil
}
