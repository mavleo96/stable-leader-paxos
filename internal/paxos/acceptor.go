package paxos

import (
	"github.com/mavleo96/stable-leader-paxos/internal/models"
	log "github.com/sirupsen/logrus"
)

// Acceptor structure handles acceptor logic for backup node
type Acceptor struct {
	id     string
	state  *ServerState
	config *ServerConfig
	peers  map[string]*models.Node

	// Components
	phaseManager *PhaseManager

	// Channels
	executionTriggerCh chan<- ExecuteRequest
}

// Reset resets the acceptor
func (a *Acceptor) Reset() {
	log.Infof("[Acceptor] Reset")
}

// CreateAcceptor creates a new acceptor
func CreateAcceptor(id string, state *ServerState, config *ServerConfig, peers map[string]*models.Node, phaseManager *PhaseManager, executionTriggerCh chan<- ExecuteRequest) *Acceptor {
	return &Acceptor{
		id:                 id,
		state:              state,
		config:             config,
		peers:              peers,
		phaseManager:       phaseManager,
		executionTriggerCh: executionTriggerCh,
	}
}
