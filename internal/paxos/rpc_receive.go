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
		log.Warnf("[PrepareRequest] Node %s is not alive", s.NodeID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Logger: Add received prepare message
	s.logger.AddReceivedPrepareMessage(req)

	// TODO: should this be part of handler?
	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.state.GetBallotNumber(), req.B) {
		log.Warnf("[PrepareRequest] Rejected %s since ballot number is lower than promised ballot number", utils.BallotNumberString(req.B))
		return nil, status.Errorf(codes.Aborted, "ballot number is lower than promised ballot number")
	}

	// Handle prepare request
	ackMessage, err := s.elector.PrepareRequestHandler(req)
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
		log.Warnf("[AcceptRequest] Node %s is not alive", s.NodeID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Logger: Add received accept message
	s.logger.AddReceivedAcceptMessage(req)

	// TODO: should this be part of handler?
	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.state.GetBallotNumber(), req.B) {
		log.Warnf("[AcceptRequest] Rejected %s since ballot number is lower than promised ballot number", utils.TransactionRequestString(req.Message))
		return nil, status.Errorf(codes.Aborted, "ballot number is lower than promised ballot number")
	}

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
		log.Warnf("[CommitRequest] Node %s is not alive", s.NodeID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Logger: Add received commit message
	s.logger.AddReceivedCommitMessage(req)

	// TODO: should this be part of handler?
	// Reject if ballot number is lower than promised ballot number
	if !BallotNumberIsHigherOrEqual(s.state.GetBallotNumber(), req.B) {
		log.Warnf("[CommitRequest] Rejected %s since ballot number is lower than promised ballot number", utils.TransactionRequestString(req.Message))
		return nil, status.Errorf(codes.Aborted, "ballot number is lower than promised ballot number")
	}

	// Handle commit request
	return s.acceptor.CommitRequestHandler(req)
}

// CatchupRequest handles the catchup request and returns the committed records
func (s *PaxosServer) CatchupRequest(ctx context.Context, req *pb.CatchupRequestMessage) (*pb.CatchupMessage, error) {
	// Ignore if not alive
	if !s.config.Alive {
		log.Warnf("[CatchupRequest] Node %s is not alive", s.NodeID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Only leader will respond to catchup request
	if s.state.GetLeader() != s.NodeID {
		log.Warnf("[CatchupRequest] Node %s is not leader", s.NodeID)
		return nil, status.Errorf(codes.Unavailable, "not leader")
	}

	// Aggregate committed records
	log.Infof("[CatchupRequest] Received catch up request from %s for seq > %d", req.NodeID, req.SequenceNum)
	catchupLog := []*pb.CommitMessage{}
	maxSequenceNum := s.state.StateLog.MaxSequenceNum()
	for sequenceNum := req.SequenceNum + 1; sequenceNum <= maxSequenceNum; sequenceNum++ {
		if !s.state.StateLog.IsCommitted(sequenceNum) {
			continue
		}
		commitMessage := &pb.CommitMessage{
			B:           s.state.StateLog.GetBallotNumber(sequenceNum),
			SequenceNum: sequenceNum,
			Message:     s.state.StateLog.GetRequest(sequenceNum),
		}
		catchupLog = append(catchupLog, commitMessage)
	}

	return &pb.CatchupMessage{
		B:         s.state.GetBallotNumber(),
		CommitLog: catchupLog,
	}, nil
}
