package clientapp

import (
	"context"
	"slices"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ReconfigureNodes reconfigures the nodes to the given alive nodes
func ReconfigureNodes(nodeMap map[string]*models.Node, aliveNodes []*models.Node) {
	log.Infof("Reconfiguring nodes to %v", nodeStringSlice(aliveNodes))
	for _, node := range nodeMap {
		status := slices.ContainsFunc(aliveNodes, func(aliveNode *models.Node) bool {
			return aliveNode.ID == node.ID
		})
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
func SendResetCommand(nodeMap map[string]*models.Node, initBalance int64) {
	log.Info("Node Reset command received")
	for _, node := range nodeMap {
		_, err := (*node.Client).ResetNode(context.Background(), &pb.ResetRequest{InitBalance: initBalance})
		if err != nil {
			log.Warnf("Error sending reset command to node %s: %v", node.ID, err)
		}
	}
}

// nodeStringSlice returns a slice of strings representing the IDs of the nodes
func nodeStringSlice(nodes []*models.Node) []string {
	nodeStrings := make([]string, 0)
	for _, node := range nodes {
		nodeStrings = append(nodeStrings, node.ID)
	}
	return nodeStrings
}
