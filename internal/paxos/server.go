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
	s.state.initializeMutex.Lock()
	defer s.state.initializeMutex.Unlock()
	if !s.state.IsSysInitialized() {
		log.Infof("[InitializeSystem] Initializing system")
		newBallotNumber := &pb.BallotNumber{N: 1, NodeID: s.ID}
		s.state.SetBallotNumber(newBallotNumber)
		elected, _ := s.elector.InitiatePrepareHandler(newBallotNumber)
		if !elected {
			log.Infof("[InitializeSystem] Election failed")
			return
		}
		go s.proposer.RunNewViewPhase(newBallotNumber, 0, nil)
		s.state.SetSysInitialized()
		log.Infof("[InitializeSystem] System initialized")
		log.Infof("[InitializeSystem] Leader is %s", s.state.GetLeader())
		return
	}
	log.Infof("[InitializeSystem] System already initialized")
}

// Start starts the paxos server
func (s *PaxosServer) Start(ctx context.Context) {
	s.wg.Go(func() { s.executor.ExecuteRouter(ctx) })
	s.wg.Go(func() { s.elector.ElectionRouter(ctx) })
	s.wg.Go(func() { s.executor.checkpointer.CheckpointPurgeRoutine(ctx) })

	s.wg.Wait()
}

// CreatePaxosServer creates a new PaxosServer instance
func CreatePaxosServer(selfNode *models.Node, peerNodes map[string]*models.Node, clients []string, bankDB *database.Database) *PaxosServer {

	serverConfig := CreateServerConfig(int64(len(peerNodes)+1), K)
	serverState := CreateServerState(selfNode.ID)

	executionTriggerCh := make(chan ExecuteRequest, 100)
	installCheckpointCh := make(chan int64, 100)

	i, err := strconv.Atoi(selfNode.ID[1:])
	if err != nil {
		log.Fatal(err)
	}
	paxosTimer := CreateSafeTimer(int64(i), int64(len(peerNodes)+1))

	logger := CreateLogger()
	checkpointer := CreateCheckpointManager(selfNode.ID, serverState, serverConfig, peerNodes)
	proposer := CreateProposer(selfNode.ID, serverState, serverConfig, peerNodes, logger, checkpointer, executionTriggerCh, installCheckpointCh)
	elector := CreateLeaderElector(selfNode.ID, serverState, serverConfig, peerNodes, paxosTimer, proposer, checkpointer, logger)
	acceptor := CreateAcceptor(selfNode.ID, serverState, serverConfig, peerNodes, paxosTimer, executionTriggerCh)
	executor := CreateExecutor(serverState, serverConfig, bankDB, checkpointer, paxosTimer, executionTriggerCh, installCheckpointCh)

	return &PaxosServer{
		Node:     selfNode,
		config:   serverConfig,
		state:    serverState,
		peers:    peerNodes,
		proposer: proposer,
		acceptor: acceptor,
		executor: executor,
		elector:  elector,
		logger:   logger,
		wg:       sync.WaitGroup{},
	}
}
