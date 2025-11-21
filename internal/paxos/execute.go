package paxos

import (
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/database"
)

type Executor struct {
	mutex              sync.Mutex
	state              *ServerState
	config             *ServerConfig
	db                 *database.Database
	timer              *SafeTimer
	executionTriggerCh chan int64
	publishTriggerCh   chan int64
	responseCh         map[int64]chan int64
}

// GetExecutionTriggerChannel is used to get the execution trigger channel
func (e *Executor) GetExecutionTriggerChannel() chan<- int64 {
	return e.executionTriggerCh
}

// AddResponseChannel is used to add a response channel for a sequence number
func (e *Executor) AddResponseChannel(sequenceNum int64, responseCh chan int64) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.responseCh[sequenceNum] = responseCh
	e.publishTriggerCh <- sequenceNum
}

// GetResponseChannel is used to get a response channel for a sequence number
func (e *Executor) GetResponseChannel(sequenceNum int64) chan int64 {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if _, ok := e.responseCh[sequenceNum]; !ok {
		return nil
	}
	return e.responseCh[sequenceNum]
}

// CloseAndRemoveResponseChannel is used to close and remove a response channel for a sequence number
func (e *Executor) CloseAndRemoveResponseChannel(sequenceNum int64) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if _, ok := e.responseCh[sequenceNum]; !ok {
		return
	}
	close(e.responseCh[sequenceNum])
	delete(e.responseCh, sequenceNum)
}

// CreateExecutor is used to create a new executor
func CreateExecutor(state *ServerState, config *ServerConfig, db *database.Database, timer *SafeTimer, executionTriggerCh chan int64) *Executor {
	return &Executor{
		mutex:              sync.Mutex{},
		state:              state,
		config:             config,
		db:                 db,
		timer:              timer,
		executionTriggerCh: executionTriggerCh,
		publishTriggerCh:   make(chan int64, 100),
		responseCh:         make(map[int64]chan int64, 10),
	}
}
