package paxos

import (
	"sync"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

// LeaderElector structure handles leader election logic
type LeaderElector struct {
	id                string
	state             *ServerState
	config            *ServerConfig
	peers             map[string]*models.Node
	prepareMessageLog *PrepareMessageLog
	timer             *SafeTimer
	proposer          *Proposer
	logger            *Logger
}

// Reset resets the leader elector
func (e *LeaderElector) Reset() {
	e.prepareMessageLog = CreatePrepareMessageLog(len(e.peers) + 1)
}

// CreateLeaderElector creates a new leader elector
func CreateLeaderElector(id string, state *ServerState, config *ServerConfig, peers map[string]*models.Node, timer *SafeTimer, proposer *Proposer, logger *Logger) *LeaderElector {
	prepareMessageLog := CreatePrepareMessageLog(len(peers) + 1)
	return &LeaderElector{
		id:                id,
		state:             state,
		config:            config,
		peers:             peers,
		prepareMessageLog: prepareMessageLog,
		timer:             timer,
		proposer:          proposer,
		logger:            logger,
	}
}

// ---------------------------------------------------------- //

// PrepareMessageLog structure handles the prepare message log
type PrepareMessageLog struct {
	mutex  sync.Mutex
	msgLog map[time.Time]*pb.PrepareMessage
	chLog  map[time.Time]chan bool
}

// AddPrepareMessage adds a prepare message to the log
func (pl *PrepareMessageLog) AddPrepareMessage(prepareMessage *pb.PrepareMessage, responseChannel chan bool) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()
	timestamp := time.Now()
	pl.msgLog[timestamp] = prepareMessage
	pl.chLog[timestamp] = responseChannel
}

// DeletePrepareMessage deletes a prepare message from the log
func (pl *PrepareMessageLog) DeletePrepareMessage(timestamp time.Time) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()
	close(pl.chLog[timestamp])
	delete(pl.msgLog, timestamp)
	delete(pl.chLog, timestamp)
}

// GetChannel gets the response channel for a prepare message
func (pl *PrepareMessageLog) GetChannel(timestamp time.Time) chan bool {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()
	return pl.chLog[timestamp]
}

// GetLogKeys gets the keys of the prepare message log
func (pl *PrepareMessageLog) GetLogKeys() []time.Time {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()
	return utils.Keys(pl.msgLog)
}

// CreatePrepareMessageLog creates a new prepare message log
func CreatePrepareMessageLog(size int) *PrepareMessageLog {
	return &PrepareMessageLog{
		mutex:  sync.Mutex{},
		msgLog: make(map[time.Time]*pb.PrepareMessage, size),
		chLog:  make(map[time.Time]chan bool, size),
	}
}
