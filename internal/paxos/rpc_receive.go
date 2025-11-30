package paxos

import (
	"context"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	"github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PrepareRequest logs the prepare request and forwards it to the handler
func (s *PaxosServer) PrepareRequest(ctx context.Context, req *pb.PrepareMessage) (*pb.AckMessage, error) {
	// Ignore if not alive
	if !s.config.Alive {
		log.Warnf("[PrepareRequest] Node %s is not alive", s.ID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Logger: Add received prepare message
	s.logger.AddReceivedPrepareMessage(req)

	// Handle prepare request
	ackMessage, err := s.phaseManager.PrepareRequestHandler(req)
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	// Logger: Add sent ack message
	s.logger.AddSentAckMessage(ackMessage)

	return ackMessage, nil
}

// AcceptRequest logs the accept request and forwards it to the handler
func (s *PaxosServer) AcceptRequest(ctx context.Context, req *pb.AcceptMessage) (*pb.AcceptedMessage, error) {
	// Ignore if not alive
	if !s.config.Alive {
		log.Warnf("[AcceptRequest] Node %s is not alive", s.ID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Logger: Add received accept message
	s.logger.AddReceivedAcceptMessage(req)

	// Handle accept request
	acceptedMessage, err := s.acceptor.AcceptRequestHandler(req)
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	// Logger: Add sent accepted message
	s.logger.AddSentAcceptedMessage(acceptedMessage)

	return acceptedMessage, nil
}

// CommitRequest logs the commit request and forwards it to the handler
func (s *PaxosServer) CommitRequest(ctx context.Context, req *pb.CommitMessage) (*emptypb.Empty, error) {
	// Ignore if not alive
	if !s.config.Alive {
		log.Warnf("[CommitRequest] Node %s is not alive", s.ID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Logger: Add received commit message
	s.logger.AddReceivedCommitMessage(req)

	// Handle commit request
	return s.acceptor.CommitRequestHandler(req)
}

// NewViewRequest handles the new view request rpc on server side
func (s *PaxosServer) NewViewRequest(req *pb.NewViewMessage, stream pb.PaxosNode_NewViewRequestServer) error {
	if !s.config.Alive {
		log.Warnf("[NewViewRequest] Node %s is not alive", s.ID)
		return status.Errorf(codes.Unavailable, "node not alive")
	}

	// Logger: Add received new view message
	s.logger.AddReceivedNewViewMessage(req)
	log.Infof("[NewViewRequest] Received new view message %v", utils.LoggingString(req.B))

	// Handle new view
	err := s.acceptor.NewViewRequestHandler(req)
	if err != nil {
		return status.Error(codes.Aborted, err.Error())
	}

	// Handle checkpoint
	checkpointSequenceNum := req.SequenceNum
	if checkpointSequenceNum > s.state.GetLastCheckpointedSequenceNum() {
		// Check if checkpoint is available
		checkpoint := s.executor.checkpointer.GetCheckpoint(checkpointSequenceNum)
		if checkpoint == nil {
			log.Warnf("[InitiatePrepareHandler] Checkpoint for sequence number %d is not available", checkpointSequenceNum)
			checkpoint, err := s.executor.checkpointer.SendGetCheckpointRequest(checkpointSequenceNum)
			if err != nil {
				log.Warnf("[InitiatePrepareHandler] Failed to get checkpoint for sequence number %d: %v", checkpointSequenceNum, err)
				return status.Error(codes.Aborted, err.Error())
			}
			s.executor.checkpointer.AddCheckpoint(checkpointSequenceNum, checkpoint.Snapshot)
		}
		signalCh := make(chan struct{}, 1)
		checkpointInstallRequest := CheckpointInstallRequest{
			SequenceNum: checkpointSequenceNum,
			SignalCh:    signalCh,
		}
		s.executor.GetInstallCheckpointChannel() <- checkpointInstallRequest
		<-signalCh
		s.executor.checkpointer.Purge(checkpointSequenceNum)
	}

	// Handle accept messages
	for _, acceptMessage := range req.AcceptLog {
		acceptedMessage, err := s.acceptor.AcceptRequestHandler(acceptMessage)
		if err != nil {
			log.Warnf("[NewViewRequest] Failed to handle accept message %v: %v", utils.LoggingString(acceptMessage), err)
			return err
		}

		// Logger: Add sent accepted message
		s.logger.AddSentAcceptedMessage(acceptedMessage)

		if err := stream.Send(acceptedMessage); err != nil {
			log.Fatal(err)
		}
	}
	log.Infof("[NewViewRequest] Finished streaming accepts for %s", utils.LoggingString(req.B))
	return nil
}

// CheckpointRequest handles the checkpoint request and forwards it to the handler
func (s *PaxosServer) CheckpointRequest(ctx context.Context, req *pb.CheckpointMessage) (*emptypb.Empty, error) {
	if !s.config.Alive {
		log.Warnf("[CheckpointRequest] Node %s is not alive", s.ID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Logger: Add received checkpoint message
	s.logger.AddReceivedCheckpointMessage(req)

	// Add checkpoint message to checkpoint log
	s.executor.checkpointer.BackupCheckpointMessageHandler(req)

	return &emptypb.Empty{}, nil
}

// ForwardRequest handles the forward request and forwards it to the leader
func (s *PaxosServer) ForwardRequest(ctx context.Context, req *pb.TransactionRequest) (*emptypb.Empty, error) {
	if !s.config.Alive {
		log.Warnf("[ForwardRequest] Node %s is not alive", s.ID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Ignore if not leader
	if !s.state.IsLeader() {
		log.Warnf("[ForwardRequest] Node %s is not leader", s.ID)
		return nil, status.Errorf(codes.Unavailable, "not leader")
	}

	// Ignore if timestamp is already processed
	timestamp, _ := s.state.DedupTable.GetLastResult(req.Sender)
	if timestamp != 0 && req.Timestamp <= timestamp {
		log.Infof("[ForwardRequest] Ignored %s since timestamp is already processed", utils.LoggingString(req))
		return &emptypb.Empty{}, nil
	}

	// Handle transaction request if not already handled
	if sequenceNum := s.state.StateLog.GetSequenceNumber(req); sequenceNum == 0 {
		s.proposer.HandleTransactionRequest(req)
	}

	return &emptypb.Empty{}, nil
}

// CatchupRequest handles the catchup request and returns the committed records
func (s *PaxosServer) CatchupRequest(ctx context.Context, req *pb.CatchupRequestMessage) (*pb.CatchupMessage, error) {
	// Ignore if not alive
	if !s.config.Alive {
		log.Warnf("[CatchupRequest] Node %s is not alive", s.ID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Only leader will respond to catchup request
	if !s.state.IsLeader() {
		log.Warnf("[CatchupRequest] Node %s is not leader", s.ID)
		return nil, status.Errorf(codes.Unavailable, "not leader")
	}

	// Logger: Add received catchup request message
	s.logger.AddReceivedCatchupRequestMessage(req)

	currentBallotNumber := s.state.GetBallotNumber()

	// Get sequence numbers and checkpoint
	log.Infof("[CatchupRequest] Received catch up request from %s for seq > %d", req.NodeID, req.SequenceNum)
	maxSequenceNum := s.state.MaxSequenceNum()
	minSequenceNum := req.SequenceNum
	var checkpoint *pb.Checkpoint
	if minSequenceNum < s.state.GetLastCheckpointedSequenceNum() {
		minSequenceNum = s.state.GetLastCheckpointedSequenceNum()
		checkpoint = s.executor.checkpointer.GetCheckpoint(minSequenceNum)
	}

	// Aggregate committed records
	catchupLog := []*pb.CommitMessage{}
	for sequenceNum := minSequenceNum + 1; sequenceNum <= maxSequenceNum; sequenceNum++ {
		if !s.state.StateLog.IsCommitted(sequenceNum) {
			continue
		}
		commitMessage := &pb.CommitMessage{
			// B:           s.state.StateLog.GetBallotNumber(sequenceNum),
			B:           currentBallotNumber,
			SequenceNum: sequenceNum,
			Message:     s.state.StateLog.GetRequest(sequenceNum),
		}
		catchupLog = append(catchupLog, commitMessage)
	}

	catchupMessage := &pb.CatchupMessage{
		B:          currentBallotNumber,
		Checkpoint: checkpoint,
		CommitLog:  catchupLog,
	}

	// Logger: Add sent catchup message
	s.logger.AddSentCatchupMessage(catchupMessage)
	log.Infof("[CatchupRequest] Sent catchup message %v", utils.LoggingString(catchupMessage))

	return catchupMessage, nil
}

// GetCheckpoint handles the get checkpoint request and returns the checkpoint
func (s *PaxosServer) GetCheckpoint(ctx context.Context, req *pb.GetCheckpointMessage) (*pb.Checkpoint, error) {
	if !s.config.Alive {
		log.Warnf("[GetCheckpoint] Node %s is not alive", s.ID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Logger: Add received get checkpoint message
	s.logger.AddReceivedGetCheckpointMessage(req)

	// Check if checkpoint is available
	checkpoint := s.executor.checkpointer.GetCheckpoint(req.SequenceNum)
	if checkpoint == nil {
		log.Warnf("[GetCheckpoint] Checkpoint for sequence number %d is not available", req.SequenceNum)
		return nil, status.Errorf(codes.NotFound, "checkpoint not available")
	}

	// Logger: Add sent checkpoint message
	s.logger.AddSentCheckpoint(checkpoint)

	return checkpoint, nil
}
