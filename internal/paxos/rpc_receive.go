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
		log.Warnf("[AcceptRequest] Node %s is not alive", s.ID)
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
		log.Warnf("[CommitRequest] Node %s is not alive", s.ID)
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

// NewViewRequest handles the new view request rpc on server side
func (s *PaxosServer) NewViewRequest(req *pb.NewViewMessage, stream pb.PaxosNode_NewViewRequestServer) error {
	if !s.config.Alive {
		log.Warnf("[NewViewRequest] Node %s is not alive", s.ID)
		return status.Errorf(codes.Unavailable, "node not alive")
	}

	// No need to acquire state mutex since AcceptRequest acquires it
	log.Infof("[NewViewRequest] Received new view message %v", utils.BallotNumberString(req.B))
	s.state.AddNewViewMessage(req)

	// Logger: Add received new view message
	s.logger.AddReceivedNewViewMessage(req)

	// Handle accept messages
	for _, acceptMessage := range req.AcceptLog {
		acceptedMessage, err := s.acceptor.AcceptRequestHandler(acceptMessage)
		if err != nil {
			log.Fatal(err)
			return err
		}

		// Logger: Add sent accepted message
		s.logger.AddSentAcceptedMessage(acceptedMessage)

		if err := stream.Send(acceptedMessage); err != nil {
			log.Fatal(err)
		}
	}
	log.Infof("[NewViewRequest] Finished streaming accepts for %s", utils.BallotNumberString(req.B))
	return nil
}

// ForwardRequest handles the forward request and forwards it to the leader
func (s *PaxosServer) ForwardRequest(ctx context.Context, req *pb.TransactionRequest) (*emptypb.Empty, error) {
	if !s.config.Alive {
		log.Warnf("[ForwardRequest] Node %s is not alive", s.ID)
		return nil, status.Errorf(codes.Unavailable, "node not alive")
	}

	// Ignore if not leader
	if s.state.GetLeader() != s.ID {
		log.Warnf("[ForwardRequest] Node %s is not leader", s.ID)
		return nil, status.Errorf(codes.Unavailable, "not leader")
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
	if s.state.GetLeader() != s.ID {
		log.Warnf("[CatchupRequest] Node %s is not leader", s.ID)
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
