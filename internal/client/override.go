package client

import (
	"context"
	"slices"

	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func ReconfigureNodes(aliveNodes []string, nodeClients map[string]pb.PaxosClient) {
	log.Infof("Reconfiguring nodes to %v", aliveNodes)
	for nodeID, nodeClients := range nodeClients {
		status := slices.Contains(aliveNodes, nodeID)
		_, err := nodeClients.ChangeNodeStatus(context.Background(), &wrapperspb.BoolValue{Value: status})
		log.Infof("Node %s status changed to %v", nodeID, status)
		if err != nil {
			log.Warn(err)
		}
	}
}

func KillLeader(nodeClients map[string]pb.PaxosClient) {
	log.Infof("Killing leader")
	for _, nodeClients := range nodeClients {
		_, err := nodeClients.KillLeader(context.Background(), &emptypb.Empty{})
		if err != nil {
			log.Warn(err)
		}
	}
}
