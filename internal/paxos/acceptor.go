package paxos

import (
	"context"
	"errors"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
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
	if !s.config.Alive {
		log.Warnf("Node is dead")
		return &pb.AckMessage{Ok: false}, status.Errorf(codes.Unavailable, "node is dead")
	}

	log.Infof("Received prepare request %v", utils.BallotNumberString(req.B))

	s.logger.AddReceivedPrepareMessage(req)

	// Reject immediately if ballot number is lower or equal to promise
	if !BallotNumberIsHigher(s.state.GetBallotNumber(), req.B) {
		log.Warnf("Rejected prepare request %s without queueing", utils.BallotNumberString(req.B))
		return &pb.AckMessage{Ok: false}, nil
	}

	// Add prepare request record to prepare message log
	prepareRequestRecord := &PrepareRequestRecord{
		ResponseChannel: make(chan *pb.PrepareMessage),
		PrepareMessage:  req,
	}
	s.PrepareMessageLog[time.Now()] = prepareRequestRecord

	if !s.SysInitialized {
		log.Infof("System not initialized, immediately accepting prepare request %s", utils.BallotNumberString(req.B))
		s.SysInitialized = true
		s.state.SetBallotNumber(req.B)
		close(prepareRequestRecord.ResponseChannel)
		prepareRequestRecord.ResponseChannel = nil
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
	acceptLog := s.state.StateLog.GetAcceptedLog()

	log.Infof("Accepted prepare request %s", utils.BallotNumberString(req.B))
	return &pb.AckMessage{
		Ok:        true,
		AcceptNum: req.B,
		AcceptLog: acceptLog,
	}, nil
}

// NewViewRequest handles the new view request rpc on server side
func (s *PaxosServer) NewViewRequest(req *pb.NewViewMessage, stream pb.PaxosNode_NewViewRequestServer) error {
	if !s.config.Alive {
		log.Warnf("Node is dead")
		return status.Errorf(codes.Unavailable, "node is dead")
	}

	// No need to acquire state mutex since AcceptRequest acquires it
	log.Infof("Received new view message %v", utils.BallotNumberString(req.B))
	s.state.AddNewViewMessage(req)

	s.logger.AddReceivedNewViewMessage(req)

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
	if !s.config.Alive {
		log.Warnf("Node is dead")
		return nil, status.Errorf(codes.Unavailable, "node is dead")
	}

	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.state.GetBallotNumber(), req.B) {
		log.Warnf("Rejected %s", utils.TransactionRequestString(req.Message))
		return &pb.AcceptedMessage{Ok: false}, nil
	}

	// Update State
	// Replace if sequence number exists in accept log else append
	// Update leader since this is a accept request with higher ballot number
	if BallotNumberIsHigher(s.state.GetBallotNumber(), req.B) {
		s.state.SetBallotNumber(req.B)
		// s.State.Leader = s.Peers[req.B.NodeID]
		// s.State.PromisedBallotNum = req.B // This is wrong according to Prajwal
	}

	created := s.state.StateLog.CreateRecordIfNotExists(req.B, req.SequenceNum, req.Message)

	// Start timer if new request
	// TODO: handle forwarding
	if created {
		s.PaxosTimer.IncrementWaitCountOrStart()
	}

	s.state.StateLog.SetAccepted(req.SequenceNum)

	s.logger.AddReceivedAcceptMessage(req)

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
	if !s.config.Alive {
		log.Warnf("Node is dead")
		return nil, status.Errorf(codes.Unavailable, "node is dead")
	}

	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.state.GetBallotNumber(), req.B) {
		log.Warnf("Rejected %s", utils.TransactionRequestString(req.Transaction))
		return &emptypb.Empty{}, nil
	}

	// Update State
	if BallotNumberIsHigher(s.state.GetBallotNumber(), req.B) {
		s.state.SetBallotNumber(req.B)
	}

	created := s.state.StateLog.CreateRecordIfNotExists(req.B, req.SequenceNum, req.Transaction)
	if created {
		s.PaxosTimer.IncrementWaitCountOrStart()
	}

	s.state.StateLog.SetCommitted(req.SequenceNum)
	log.Infof("Commited %s", utils.TransactionRequestString(req.Transaction))

	s.logger.AddReceivedCommitMessage(req)

	// Try to execute the request
	if s.state.StateLog.IsExecuted(req.SequenceNum) {
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

	if !s.config.Alive {
		log.Warnf("Node is dead")
		return nil, status.Errorf(codes.Unavailable, "node is dead")
	}

	// Only leader will respond to catchup request
	if s.state.GetLeader() != s.NodeID {
		return nil, status.Errorf(codes.Aborted, "not leader")
	}

	log.Infof("Received catch up request logs with sequence number >%d", req.Value)
	catchupLog := []*pb.AcceptRecord{}
	maxSequenceNum := s.state.StateLog.MaxSequenceNum()

	for sequenceNum := req.Value + 1; sequenceNum <= maxSequenceNum; sequenceNum++ {
		// record, ok := s.State.AcceptLog[sequenceNum]
		// if !ok || !record.Committed {
		if !s.state.StateLog.IsCommitted(sequenceNum) {
			continue
		}
		catchupLog = append(catchupLog, &pb.AcceptRecord{
			AcceptedBallotNum:   s.state.StateLog.GetBallotNumber(sequenceNum),
			AcceptedSequenceNum: sequenceNum,
			AcceptedVal:         s.state.StateLog.GetRequest(sequenceNum),
			Committed:           s.state.StateLog.IsCommitted(sequenceNum),
			Executed:            s.state.StateLog.IsExecuted(sequenceNum),
			Result:              utils.Int64ToBool(s.state.StateLog.GetResult(sequenceNum)),
		})
	}

	log.Infof("Catchup sent %d records", len(catchupLog))
	return &pb.CatchupMessage{
		B:         s.state.GetBallotNumber(),
		AcceptLog: catchupLog,
	}, nil
}

// TryExecute tries to execute the transaction
// The state mutex should be acquired before calling this function
func (s *PaxosServer) TryExecute(sequenceNum int64) (bool, error) {
	// Execute transactions until the sequence number is executed
	for s.state.GetLastExecutedSequenceNum() < sequenceNum {
		nextSequenceNum := s.state.GetLastExecutedSequenceNum() + 1
		if !s.state.StateLog.IsCommitted(nextSequenceNum) {
			return false, errors.New("not executed since log has gaps")
		}
		if s.state.StateLog.IsExecuted(nextSequenceNum) {
			log.Fatal("Executed sequence number is already executed")
		}
		request := s.state.StateLog.GetRequest(nextSequenceNum)
		if request == nil {
			return false, errors.New("request is nil")
		}

		// Update database
		var result int64
		var err error
		if request != NoOperation {
			var success bool
			success, err = s.DB.UpdateDB(request.Transaction)
			if err != nil {
				log.Warn(err)
			}
			result = utils.BoolToInt64(success)
		}

		// Add to executed log
		s.state.StateLog.SetExecuted(nextSequenceNum)
		s.state.StateLog.SetResult(nextSequenceNum, result)

		log.Infof("Executed %s", utils.TransactionRequestString(request))
		s.state.SetLastExecutedSequenceNum(nextSequenceNum)
	}
	return utils.Int64ToBool(s.state.StateLog.GetResult(sequenceNum)), nil
}
