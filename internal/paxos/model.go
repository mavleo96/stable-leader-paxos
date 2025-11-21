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
	config              *ServerConfig
	state               *ServerState
	peers               map[string]*models.Node
	SysInitializedMutex sync.Mutex
	SysInitialized      bool

	// Component Managers
	proposer *Proposer
	acceptor *Acceptor
	executor *Executor
	elector  *LeaderElector
	logger   *Logger

	// Wait group for the server
	wg sync.WaitGroup

	// UnimplementedPaxosNodeServer is the server interface for the PaxosNode service
	pb.UnimplementedPaxosNodeServer
}

// InitializeSystem initializes the system by leader election
func (s *PaxosServer) InitializeSystem() {
	s.SysInitializedMutex.Lock()
	defer s.SysInitializedMutex.Unlock()
	if !s.SysInitialized {
		log.Infof("Initializing system by leader election")
		// s.PrepareRoutine()
		s.SysInitialized = true
		log.Infof("System initialized")
		return
	}
	log.Infof("System already initialized")
}

// Start starts the paxos server
func (s *PaxosServer) Start(ctx context.Context) {
	s.wg.Go(func() { s.executor.ExecuteRouter(ctx) })
	s.wg.Go(func() { s.elector.ElectionRouter(ctx) })

	s.wg.Wait()
}

// CreatePaxosServer creates a new PaxosServer instance
func CreatePaxosServer(selfNode *models.Node, peerNodes map[string]*models.Node, clients []string, bankDB *database.Database) *PaxosServer {

	serverConfig := CreateServerConfig(int64(len(peerNodes) + 1))
	serverState := CreateServerState(selfNode.ID)

	executionTriggerCh := make(chan ExecuteRequest, 100)

	i, err := strconv.Atoi(selfNode.ID[1:])
	if err != nil {
		log.Fatal(err)
	}
	paxosTimer := CreateSafeTimer(int64(i), int64(len(peerNodes)+1))

	logger := CreateLogger()
	proposer := CreateProposer(selfNode.ID, serverState, serverConfig, peerNodes, executionTriggerCh, logger)
	elector := CreateLeaderElector(selfNode.ID, serverState, serverConfig, peerNodes, paxosTimer, proposer, logger)
	acceptor := CreateAcceptor(selfNode.ID, serverState, serverConfig, peerNodes, paxosTimer, executionTriggerCh)
	executor := CreateExecutor(serverState, serverConfig, bankDB, paxosTimer, executionTriggerCh)

	return &PaxosServer{
		Node:                selfNode,
		config:              serverConfig,
		state:               serverState,
		peers:               peerNodes,
		SysInitializedMutex: sync.Mutex{},
		SysInitialized:      true,
		proposer:            proposer,
		acceptor:            acceptor,
		executor:            executor,
		elector:             elector,
		logger:              logger,
		wg:                  sync.WaitGroup{},
	}
}
