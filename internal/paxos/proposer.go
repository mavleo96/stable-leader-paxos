package paxos

import (
	"github.com/mavleo96/stable-leader-paxos/internal/models"
	log "github.com/sirupsen/logrus"
)

// Proposer structure handles proposer logic for leader node
type Proposer struct {
	id     string
	state  *ServerState
	config *ServerConfig
	peers  map[string]*models.Node

	// Components
	logger       *Logger
	checkpointer *CheckpointManager
	phaseManager *PhaseManager

	// Channels and context
	executionTriggerCh  chan ExecuteRequest
	installCheckpointCh chan int64
}

// Reset resets the proposer
func (p *Proposer) Reset() {
	log.Infof("[Proposer] Reset")
}

// CreateProposer creates a new proposer
func CreateProposer(id string, state *ServerState, config *ServerConfig, peers map[string]*models.Node, logger *Logger, checkpointer *CheckpointManager, phaseManager *PhaseManager, executionTriggerCh chan ExecuteRequest, installCheckpointCh chan int64) *Proposer {
	return &Proposer{
		id:                  id,
		state:               state,
		config:              config,
		peers:               peers,
		logger:              logger,
		checkpointer:        checkpointer,
		phaseManager:        phaseManager,
		executionTriggerCh:  executionTriggerCh,
		installCheckpointCh: installCheckpointCh,
	}
}
