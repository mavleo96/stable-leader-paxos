package models

import (
	"github.com/mavleo96/stable-leader-paxos/internal/config"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	"github.com/mavleo96/stable-leader-paxos/pb"
)

// Node represents a node in the distributed system
type Node struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
	Client  *pb.PaxosNodeClient
	Close   func() error
}

// GetNodeMap creates a map of nodes from a map of node configurations
func GetNodeMap(nodeConfig map[string]*config.NodeEntry) (map[string]*Node, error) {
	nodeMap := make(map[string]*Node)
	for id, nodeConfig := range nodeConfig {

		// Connect to node
		conn, err := utils.Connect(nodeConfig.Address)
		if err != nil {
			return nil, err
		}
		nodeClient := pb.NewPaxosNodeClient(conn)

		// Create node struct
		nodeMap[id] = &Node{
			ID:      id,
			Address: nodeConfig.Address,
			Client:  &nodeClient,
			Close:   func() error { return conn.Close() },
		}
	}
	return nodeMap, nil
}
