package paxos

import (
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/database"
)

// Executor represents the executor for the Paxos server
type Executor struct {
	mutex  sync.Mutex
	state  *ServerState
	config *ServerConfig

	// Components
	checkpointer *CheckpointManager
	timer        *SafeTimer

	// Database
	db *database.Database

	// Channels
	executionTriggerCh  chan ExecuteRequest
	installCheckpointCh chan int64
}

// ExecuteRequest is a request to execute a transaction
type ExecuteRequest struct {
	SequenceNum int64
	SignalCh    chan bool
}

// GetExecutionTriggerChannel is used to get the execution trigger channel
func (e *Executor) GetExecutionTriggerChannel() chan<- ExecuteRequest {
	return e.executionTriggerCh
}

// GetInstallCheckpointChannel is used to get the install checkpoint channel
func (e *Executor) GetInstallCheckpointChannel() chan<- int64 {
	return e.installCheckpointCh
}

// Reset resets the executor
func (e *Executor) Reset() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
}

// CreateExecutor is used to create a new executor
func CreateExecutor(state *ServerState, config *ServerConfig, db *database.Database, checkpointer *CheckpointManager, timer *SafeTimer, executionTriggerCh chan ExecuteRequest, installCheckpointCh chan int64) *Executor {
	return &Executor{
		mutex:               sync.Mutex{},
		state:               state,
		config:              config,
		checkpointer:        checkpointer,
		timer:               timer,
		db:                  db,
		executionTriggerCh:  executionTriggerCh,
		installCheckpointCh: installCheckpointCh,
	}
}
