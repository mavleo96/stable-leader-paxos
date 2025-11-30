package paxos

import (
	"context"
	"strconv"
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/database"
	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// PaxosServer represents the Paxos server node
type PaxosServer struct {
	*models.Node
	config *ServerConfig
	state  *ServerState
	peers  map[string]*models.Node

	// Component Managers
	proposer     *Proposer
	acceptor     *Acceptor
	executor     *Executor
	logger       *Logger
	phaseManager *PhaseManager

	// Wait group for the server
	wg sync.WaitGroup

	// UnimplementedPaxosNodeServer is the server interface for the PaxosNode service
	pb.UnimplementedPaxosNodeServer
}

// Start starts the paxos server
func (s *PaxosServer) Start(ctx context.Context) {
	s.wg.Go(func() { s.executor.ExecuteRouter(ctx) })
	s.wg.Go(func() { s.phaseManager.PhaseTimeoutRoutine(ctx) })

	s.wg.Wait()
}

// CreatePaxosServer creates a new PaxosServer instance
func CreatePaxosServer(selfNode *models.Node, peerNodes map[string]*models.Node, clients []string, bankDB *database.Database) *PaxosServer {

	serverConfig := CreateServerConfig(int64(len(peerNodes)+1), K)
	serverState := CreateServerState(selfNode.ID)

	executionTriggerCh := make(chan ExecuteRequest, 100)
	installCheckpointCh := make(chan CheckpointInstallRequest, 100)

	i, err := strconv.ParseInt(selfNode.ID[1:], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	paxosTimer := CreateSafeTimer(i, int64(len(peerNodes)+1))

	logger := CreateLogger()
	phaseManager := CreatePhaseManager(selfNode.ID, serverState, paxosTimer)
	checkpointer := CreateCheckpointManager(selfNode.ID, serverState, serverConfig, peerNodes, logger)
	proposer := CreateProposer(selfNode.ID, serverState, serverConfig, peerNodes, phaseManager, checkpointer, logger, executionTriggerCh, installCheckpointCh)
	acceptor := CreateAcceptor(selfNode.ID, serverState, serverConfig, peerNodes, phaseManager, checkpointer, executionTriggerCh)
	executor := CreateExecutor(serverState, serverConfig, bankDB, checkpointer, paxosTimer, executionTriggerCh, installCheckpointCh)

	server := PaxosServer{
		Node:         selfNode,
		config:       serverConfig,
		state:        serverState,
		peers:        peerNodes,
		proposer:     proposer,
		acceptor:     acceptor,
		executor:     executor,
		logger:       logger,
		phaseManager: phaseManager,
		wg:           sync.WaitGroup{},
	}
	server.phaseManager.initiatePrepareHandler = proposer.InitiatePrepareHandler
	return &server
}
