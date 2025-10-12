package main

import (
	"flag"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mavleo96/cft-mavleo96/internal/config"
	"github.com/mavleo96/cft-mavleo96/internal/database"
	"github.com/mavleo96/cft-mavleo96/internal/models"
	"github.com/mavleo96/cft-mavleo96/internal/paxos"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"

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

	// Find self node configuration
	var selfNode *models.Node
	for _, node := range cfg.Nodes {
		if node.ID == *id {
			selfNode = node
			break
		}
	}
	if selfNode == nil {
		log.Fatal("Node ID not found in config")
	}

	// Create database
	bankDB := &database.Database{}
	log.Info("Initializing database at " + cfg.DBDir + "/" + *id + ".db" + " with clients " + strings.Join(cfg.Clients, ", "))
	err = bankDB.InitDB(cfg.DBDir+"/"+*id+".db", cfg.Clients)
	if err != nil {
		log.Fatal(err)
	}
	defer bankDB.Close()
	log.Infof("Database initialized at %s", cfg.DBDir+"/"+*id+".db")

	// Create gRPC server
	lis, err := net.Listen("tcp", selfNode.Address)
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()

	// Initialize last reply map and peers map
	lastReply := make(map[string]*pb.TransactionResponse)
	for _, client := range cfg.Clients {
		lastReply[client] = nil
	}
	peers := make(map[string]*models.Node)
	for _, node := range cfg.Nodes {
		peers[node.ID] = node
	}

	// Initialize paxos timer
	i, err := strconv.Atoi(selfNode.ID[1:])
	if err != nil {
		log.Fatal(err)
	}
	paxosTimer := paxos.CreateSafeTimer(i, len(cfg.Nodes))

	// Initialize paxos server
	paxosServer := paxos.PaxosServer{
		Mutex:   sync.RWMutex{},
		IsAlive: true,
		NodeID:  selfNode.ID,
		Addr:    selfNode.Address,
		State: paxos.AcceptorState{
			Mutex:               sync.RWMutex{},
			Leader:              &models.Node{ID: "n1", Address: "localhost:5001"},
			PromisedBallotNum:   &pb.BallotNumber{N: 1, NodeID: "n1"},
			AcceptLog:           make(map[int64]*pb.AcceptRecord),
			ExecutedSequenceNum: 0,
		},
		DB:                bankDB,
		LastReply:         lastReply,
		Peers:             peers,
		Quorum:            len(cfg.Nodes)/2 + 1,
		PrepareMessageLog: make(map[time.Time]*paxos.PrepareRequestRecord),
		PaxosTimer:        paxosTimer,
	}

	// Register paxos server
	pb.RegisterPaxosServer(grpcServer, &paxosServer)

	// Start gRPC server and paxos server timeout routine
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	})
	wg.Go(func() {
		paxosServer.ServerTimeoutRoutine()
	})
	log.Infof("gRPC server listening on %s", selfNode.Address)

	// Wait for gRPC server and paxos server timeout routine to finish
	wg.Wait()
}
