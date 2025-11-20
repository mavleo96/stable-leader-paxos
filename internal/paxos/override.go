package paxos

import (
	"context"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ChangeNodeStatus sets the node status to active or inactive
func (s *PaxosServer) ChangeNodeStatus(ctx context.Context, status *wrapperspb.BoolValue) (*emptypb.Empty, error) {
	if s.config.Alive == status.Value {
		return &emptypb.Empty{}, nil
	}
	s.config.Alive = status.Value
	s.acceptor.timer.Cleanup()
	if s.config.Alive {
		go s.acceptor.timer.run()
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
	log.Warnf("Node %s status changed to %v", s.ID, s.config.Alive)
	return &emptypb.Empty{}, nil
}
