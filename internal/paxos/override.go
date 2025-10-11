package paxos

import (
	"context"

	"github.com/mavleo96/cft-mavleo96/internal/models"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ChangeNodeStatus sets the node status to active or inactive
func (s *PaxosServer) ChangeNodeStatus(ctx context.Context, status *wrapperspb.BoolValue) (*emptypb.Empty, error) {
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()
	if s.IsAlive == status.Value {
		return &emptypb.Empty{}, nil
	}
	s.IsAlive = status.Value
	s.PaxosTimer.TimerCleanup()
	log.Warnf("Node %s status changed to %v", s.NodeID, s.IsAlive)
	return &emptypb.Empty{}, nil
}

// KillLeader kills the leader of the cluster
func (s *PaxosServer) KillLeader(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()
	if s.State.Leader.ID != s.NodeID {
		return &emptypb.Empty{}, nil
	}
	s.IsAlive = false
	s.State.Leader = &models.Node{ID: ""}
	s.PaxosTimer.TimerCleanup()
	log.Warnf("Node %s status changed to %v", s.NodeID, s.IsAlive)
	return &emptypb.Empty{}, nil
}
