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

	executeQueue map[int64][]ExecuteRequest // sequence number -> execute requests

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

// Enqueue is used to enqueue an execute request
func (e *Executor) enqueue(sequenceNum int64, executeRequest ExecuteRequest) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Create queue if not exists
	if _, ok := e.executeQueue[sequenceNum]; !ok {
		e.executeQueue[sequenceNum] = make([]ExecuteRequest, 0)
	}
	e.executeQueue[sequenceNum] = append(e.executeQueue[sequenceNum], executeRequest)
}

// Dequeue is used to dequeue execute requests for a given sequence number
func (e *Executor) dequeue(sequenceNum int64) []ExecuteRequest {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	executeRequests := e.executeQueue[sequenceNum]
	delete(e.executeQueue, sequenceNum)
	return executeRequests
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
	e.executeQueue = make(map[int64][]ExecuteRequest)
}

// CreateExecutor is used to create a new executor
func CreateExecutor(state *ServerState, config *ServerConfig, db *database.Database, checkpointer *CheckpointManager, timer *SafeTimer, executionTriggerCh chan ExecuteRequest, installCheckpointCh chan int64) *Executor {
	return &Executor{
		mutex:               sync.Mutex{},
		state:               state,
		config:              config,
		executeQueue:        make(map[int64][]ExecuteRequest),
		checkpointer:        checkpointer,
		timer:               timer,
		db:                  db,
		executionTriggerCh:  executionTriggerCh,
		installCheckpointCh: installCheckpointCh,
	}
}
