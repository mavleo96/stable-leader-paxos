package paxos

import (
	"context"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ChangeNodeStatus sets the node status to active or inactive
func (s *PaxosServer) ChangeNodeStatus(ctx context.Context, status *wrapperspb.BoolValue) (*emptypb.Empty, error) {
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
		// go s.acceptor.timer.run()
		go s.CatchupRoutine()
	}
	log.Warnf("Node %s status changed to %v", s.ID, s.config.Alive)
	return &emptypb.Empty{}, nil
}

// KillLeader kills the leader of the cluster
func (s *PaxosServer) KillLeader(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	if s.state.GetLeader() != s.ID {
		return &emptypb.Empty{}, nil
	}
	s.config.Alive = false
	s.state.SetLeader("")
	log.Warnf("Node %s status changed to %v", s.ID, s.config.Alive)
	return &emptypb.Empty{}, nil
}
