package paxos

import (
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/database"
)

// Executor represents the executor for the Paxos server
type Executor struct {
	mutex              sync.Mutex
	state              *ServerState
	config             *ServerConfig
	db                 *database.Database
	timer              *SafeTimer
	executionTriggerCh chan ExecuteRequest
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

// Reset resets the executor
func (e *Executor) Reset() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
}

// CreateExecutor is used to create a new executor
func CreateExecutor(state *ServerState, config *ServerConfig, db *database.Database, timer *SafeTimer, executionTriggerCh chan ExecuteRequest) *Executor {
	return &Executor{
		mutex:              sync.Mutex{},
		state:              state,
		config:             config,
		db:                 db,
		timer:              timer,
		executionTriggerCh: executionTriggerCh,
	}
}
