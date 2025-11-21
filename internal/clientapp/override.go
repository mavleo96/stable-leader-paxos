package clientapp

import (
	"context"
	"slices"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ReconfigureNodes reconfigures the nodes to the given alive nodes
func ReconfigureNodes(nodeMap map[string]*models.Node, aliveNodes []string) {
	log.Infof("Reconfiguring nodes to %v", aliveNodes)
	for _, node := range nodeMap {
		status := slices.Contains(aliveNodes, node.ID)
		_, err := (*node.Client).ReconfigureNode(context.Background(), &wrapperspb.BoolValue{Value: status})
		if err != nil {
			log.Warnf("Error reconfiguring node %s: %v", node.ID, err)
		}
	}
}

// KillLeader kills the leader of the cluster
func KillLeader(nodeMap map[string]*models.Node) {
	log.Infof("Killing leader")
	for _, node := range nodeMap {
		_, err := (*node.Client).KillLeader(context.Background(), &emptypb.Empty{})
		if err != nil {
			log.Warnf("Error killing leader on node %s: %v", node.ID, err)
		}
	}
}

// SendResetCommand sends a reset command to all nodes
func SendResetCommand(nodeMap map[string]*models.Node) {
	log.Info("Node Reset command received")
	for _, node := range nodeMap {
		_, err := (*node.Client).ResetNode(context.Background(), &emptypb.Empty{})
		if err != nil {
			log.Warnf("Error sending reset command to node %s: %v", node.ID, err)
		}
	}
}
