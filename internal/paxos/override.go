package paxos

import (
	"context"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ReconfigureNode reconfigures the node to active or inactive
func (s *PaxosServer) ReconfigureNode(ctx context.Context, status *wrapperspb.BoolValue) (*emptypb.Empty, error) {
	// If node status is already the same, return
	if s.config.Alive == status.Value {
		return &emptypb.Empty{}, nil
	}

	// If node is alive, set leader to empty string and start timer and catchup routine
	if status.Value {
		s.InitiateCatchupHandler()
		s.config.Alive = true
	} else {
		s.config.Alive = false
		s.phaseManager.timer.StopIfRunning()
	}
	log.Warnf("Node %s status changed to %v", s.ID, s.config.Alive)
	return &emptypb.Empty{}, nil
}

// KillLeader kills the leader of the cluster
func (s *PaxosServer) KillLeader(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	if !s.state.IsLeader() {
		return &emptypb.Empty{}, nil
	}
	s.config.Alive = false
	s.phaseManager.CancelProposerCtx()
	s.phaseManager.timer.StopIfRunning()
	log.Warnf("Node %s status changed to %v", s.ID, s.config.Alive)
	return &emptypb.Empty{}, nil
}

// ResetNode resets the server state and database
func (s *PaxosServer) ResetNode(ctx context.Context, req *pb.ResetRequest) (*emptypb.Empty, error) {
	// Reset server state
	s.state.Reset()
	s.acceptor.Reset()
	s.proposer.Reset()
	s.phaseManager.Reset()
	s.executor.Reset()
	s.executor.db.ResetDB(req.InitBalance)
	s.executor.checkpointer.Reset()
	s.logger.Reset()

	return &emptypb.Empty{}, nil
}
