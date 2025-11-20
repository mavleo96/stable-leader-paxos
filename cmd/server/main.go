package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/mavleo96/stable-leader-paxos/internal/config"
	"github.com/mavleo96/stable-leader-paxos/internal/database"
	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/paxos"
	pb "github.com/mavleo96/stable-leader-paxos/pb"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	log.SetFormatter(&log.TextFormatter{TimestampFormat: "15:04.000"})

	id := flag.String("id", "n1", "Node ID")
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	// Load configurations
	// Peer nodes and their addresses, clients, database directory are read from config file
	cfg, err := config.ParseConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	nodeMap := models.GetNodeMap(cfg.Nodes)
	if err != nil {
		log.Fatal(err)
	}

	// Find self node configuration
	selfNode, ok := nodeMap[*id]
	if !ok {
		log.Fatal("Node ID not found in config")
	}
	log.Infof("Self node configuration: %s", selfNode.ID)
	peerNodes := make(map[string]*models.Node)
	for _, node := range nodeMap {
		if node.ID != *id {
			peerNodes[node.ID] = node
		}
	}

	// Create context for graceful shutdown (will be cancelled on signal)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverData, err := CreateServer(selfNode, peerNodes, cfg.Clients, cfg.DBDir, cfg.InitBalance, ctx)
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := serverData.grpcServer
	bankDB := serverData.bankDB

	lis, err := net.Listen("tcp", selfNode.Address)
	if err != nil {
		log.Fatal(err)
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Start gRPC server in a goroutine
	var wg sync.WaitGroup
	var serveErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Infof("gRPC server listening on %s", selfNode.Address)
		if err := grpcServer.Serve(lis); err != nil {
			serveErr = err
			// Only log the error if it's from closing the listener
			if err.Error() != "use of closed network connection" {
				log.Errorf("gRPC server error: %v", err)
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Info("Received shutdown signal, shutting down gracefully...")

	// Cancel context to stop paxos server
	cancel()

	// Stop gRPC server gracefully (waits for existing RPCs to complete)
	// This stops accepting new connections and waits for existing RPCs to finish
	log.Info("Stopping gRPC server...")
	grpcServer.GracefulStop()
	log.Info("gRPC server stopped")

	// Close all node connections
	log.Info("Closing node connections...")
	for _, node := range nodeMap {
		if err := node.Close(); err != nil {
			log.Warnf("Error closing node connection %s: %v", node.ID, err)
		}
	}

	// Close database
	if bankDB != nil {
		log.Info("Closing database...")
		if err := bankDB.Close(); err != nil {
			log.Warnf("Error closing database: %v", err)
		}
	}

	// Wait for gRPC server to finish
	wg.Wait()

	if serveErr != nil && serveErr.Error() != "use of closed network connection" {
		log.Fatalf("gRPC server error: %v", serveErr)
	}

	log.Info("Server shut down complete")
}

// ServerData represents the data needed to create a server
type ServerData struct {
	grpcServer *grpc.Server
	bankDB     *database.Database
	paxosNode  *paxos.PaxosServer
}

// CreateServer creates a server with the given data
func CreateServer(selfNode *models.Node, peerNodes map[string]*models.Node, clients []string, dbDir string, initBalance int64, ctx context.Context) (*ServerData, error) {

	bankDB := &database.Database{}
	dbPath := filepath.Join(dbDir, selfNode.ID+".db")
	log.Infof("Initializing database at %s", dbPath)
	if err := bankDB.InitDB(dbPath, clients, initBalance); err != nil {
		return nil, err
	}

	grpcServer := grpc.NewServer()

	node := paxos.CreatePaxosServer(selfNode, peerNodes, clients, bankDB)
	log.Infof("[Main] Created paxos server with peer nodes: %v", peerNodes)
	pb.RegisterPaxosNodeServer(grpcServer, node)

	// Start paxos server (it will stop when ctx is cancelled)
	go node.Start(ctx)

	return &ServerData{
		grpcServer: grpcServer,
		bankDB:     bankDB,
		paxosNode:  node,
	}, nil
}
