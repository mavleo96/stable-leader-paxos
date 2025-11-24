package paxos

import (
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/crypto"
	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

// CheckpointManager is responsible for managing check point messages
type CheckpointManager struct {
	mutex  sync.RWMutex
	id     string
	state  *ServerState
	config *ServerConfig
	peers  map[string]*models.Node

	// checkpoints
	checkpointMessageLog     map[int64]*pb.CheckpointMessage
	checkpoints              map[int64]*pb.Checkpoint
	checkpointPurgeRoutineCh chan int64
}

// GetCheckpointPurgeRoutineCh gets the checkpoint purge routine channel
func (c *CheckpointManager) GetCheckpointPurgeRoutineCh() chan<- int64 {
	return c.checkpointPurgeRoutineCh
}

// AddCheckpointMessage adds a checkpoint message for a given sequence number
func (c *CheckpointManager) AddCheckpointMessage(sequenceNum int64, checkpointMessage *pb.CheckpointMessage) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.checkpointMessageLog[sequenceNum] = checkpointMessage
}

// GetCheckpointMessage gets a checkpoint message for a given sequence number
func (c *CheckpointManager) GetCheckpointMessage(sequenceNum int64) *pb.CheckpointMessage {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.checkpointMessageLog[sequenceNum]
}

// DeleteCheckpointMessage deletes a checkpoint message for a given sequence number
func (c *CheckpointManager) DeleteCheckpointMessage(sequenceNum int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.checkpointMessageLog, sequenceNum)
}

// AddCheckpoint adds a checkpoint for a given sequence number
func (c *CheckpointManager) AddCheckpoint(sequenceNum int64, snapshot map[string]int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	digest := crypto.DigestAny(snapshot)
	c.checkpoints[sequenceNum] = &pb.Checkpoint{
		SequenceNum: sequenceNum,
		Digest:      digest,
		Snapshot:    snapshot,
	}
}

// GetCheckpoint gets the checkpoint for a given sequence number
func (c *CheckpointManager) GetCheckpoint(sequenceNum int64) *pb.Checkpoint {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.checkpoints[sequenceNum]
}

// DeleteCheckpoint deletes the checkpoint for a given sequence number
func (c *CheckpointManager) DeleteCheckpoint(sequenceNum int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.checkpoints, sequenceNum)
}

// Reset resets the checkpoint manager
func (c *CheckpointManager) Reset() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.checkpoints = make(map[int64]*pb.Checkpoint, 5)
}

// CreateCheckpointManager creates a new check point manager
func CreateCheckpointManager(id string, state *ServerState, config *ServerConfig, peers map[string]*models.Node) *CheckpointManager {
	return &CheckpointManager{
		mutex:                    sync.RWMutex{},
		id:                       id,
		state:                    state,
		config:                   config,
		peers:                    peers,
		checkpointMessageLog:     make(map[int64]*pb.CheckpointMessage, 5),
		checkpoints:              make(map[int64]*pb.Checkpoint, 5),
		checkpointPurgeRoutineCh: make(chan int64, 100),
	}
}
