package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mavleo96/cft-mavleo96/internal/config"
	"github.com/mavleo96/cft-mavleo96/internal/models"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

func main() {
	id := flag.String("id", "n1", "Node ID")
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	// Load configurations
	// Peer nodes and their addresses, clients, database directory are read from config file
	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatal(err)
		return
	}
	var cfg config.Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		log.Fatal(err)
		return
	}
	// Find self node configuration
	var selfNode *models.Node
	for _, node := range cfg.Nodes {
		if node.ID == *id {
			selfNode = &node
			break
		}
	}
	if selfNode == nil {
		log.Fatal("Node ID not found in config")
		return
	}

	// Print loaded configuration for verification
	fmt.Printf("Node ID: %s\n", selfNode.ID)
	fmt.Printf("Node Address: %s\n", selfNode.Address)
	fmt.Printf("Clients: %v\n", cfg.Clients)
	fmt.Printf("Database Directory: %s\n", cfg.DBDir)
	fmt.Printf("Peer Nodes: %v\n", cfg.Nodes)
}
