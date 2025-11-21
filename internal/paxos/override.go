package paxos

import (
	"context"

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

	// Set node status and cleanup timer
	s.config.Alive = status.Value
	s.acceptor.timer.Cleanup()

	// If node is alive, set leader to empty string and start timer and catchup routine
	if s.config.Alive {
		s.state.SetLeader("")
		go s.CatchupRoutine()
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
	s.state.SetLeader("")
	log.Warnf("Node %s status changed to %v", s.ID, s.config.Alive)
	return &emptypb.Empty{}, nil
}

// ResetNode resets the server state and database
func (s *PaxosServer) ResetNode(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	// Reset server state
	s.state.Reset()
	s.executor.db.ResetDB(10)
	s.executor.Reset()
	s.elector.Reset()
	s.logger.Reset()

	return &emptypb.Empty{}, nil
}
