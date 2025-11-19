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
	s.PaxosTimer.TimerCleanup()
	if s.config.Alive {
		// s.State.Leader = &models.Node{ID: ""}
		go s.PaxosTimer.timerRoutine()
		go s.CatchupRoutine()
	}
	log.Warnf("Node %s status changed to %v", s.NodeID, s.config.Alive)
	return &emptypb.Empty{}, nil
}

// KillLeader kills the leader of the cluster
func (s *PaxosServer) KillLeader(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	// s.State.Mutex.Lock()
	// defer s.State.Mutex.Unlock()
	// if s.State.Leader.ID != s.NodeID {
	// 	return &emptypb.Empty{}, nil
	// }
	// s.IsAlive = false
	// s.State.Leader = &models.Node{ID: ""}
	// s.PaxosTimer.TimerCleanup()
	// log.Warnf("Node %s status changed to %v", s.NodeID, s.IsAlive)
	// return &emptypb.Empty{}, nil

	if s.state.GetLeader() != s.NodeID {
		return &emptypb.Empty{}, nil
	}
	s.config.Alive = false
	s.PaxosTimer.TimerCleanup()
	log.Warnf("Node %s status changed to %v", s.NodeID, s.config.Alive)
	return &emptypb.Empty{}, nil
}
