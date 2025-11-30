package paxos

import (
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/database"
)

// Executor represents the executor for the Paxos server
type Executor struct {
	mutex        sync.RWMutex
	state        *ServerState
	config       *ServerConfig
	executeQueue map[int64][]ExecuteRequest // sequence number -> execute requests

	// Components
	checkpointer *CheckpointManager
	timer        *SafeTimer

	// Database
	db *database.Database

	// Channels
	executionTriggerCh  chan ExecuteRequest
	installCheckpointCh chan CheckpointInstallRequest
}

// ExecuteRequest is a request to execute a transaction
type ExecuteRequest struct {
	SequenceNum int64
	ResultCh    chan<- int64
}

// CheckpointInstallRequest is a request to install a checkpoint
type CheckpointInstallRequest struct {
	SequenceNum int64
	SignalCh    chan<- struct{}
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
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.executionTriggerCh
}

// GetInstallCheckpointChannel is used to get the install checkpoint channel
func (e *Executor) GetInstallCheckpointChannel() chan<- CheckpointInstallRequest {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.installCheckpointCh
}

// Reset resets the executor
func (e *Executor) Reset() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.executeQueue = make(map[int64][]ExecuteRequest)
}

// CreateExecutor is used to create a new executor
func CreateExecutor(state *ServerState, config *ServerConfig, db *database.Database, checkpointer *CheckpointManager, timer *SafeTimer, executionTriggerCh chan ExecuteRequest, installCheckpointCh chan CheckpointInstallRequest) *Executor {
	return &Executor{
		mutex:               sync.RWMutex{},
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
