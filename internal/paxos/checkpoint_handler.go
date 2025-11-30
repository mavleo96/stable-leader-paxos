package paxos

import (
	"bytes"
	"context"
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BackupCheckpointMessageHandler adds the checkpoint message to the checkpoint message log and tries to purge the logs, checkpoints, and checkpoint messages if the digest is the same
func (c *CheckpointManager) BackupCheckpointMessageHandler(checkpointMessage *pb.CheckpointMessage) {
	sequenceNum := checkpointMessage.SequenceNum

	// Add check point message to check point log if higher or equal to last checkpointed sequence number
	if sequenceNum <= c.state.GetLastCheckpointedSequenceNum() {
		log.Warnf("[CheckpointMessageHandler] Check point message for sequence number %d is not higher than last checkpointed sequence number (%d)", sequenceNum, c.state.GetLastCheckpointedSequenceNum())
		return
	}

	// Add checkpoint message to checkpoint message log
	c.AddCheckpointMessage(sequenceNum, checkpointMessage)
	log.Infof("[CheckpointMessageHandler] Checkpoint message for sequence number %d is logged", sequenceNum)

	c.BackupTryCheckpointHandler(sequenceNum)
}

// BackupTryCheckpointHandler tries to purge the logs, checkpoints, and checkpoint messages if the digest is the same
func (c *CheckpointManager) BackupTryCheckpointHandler(sequenceNum int64) {
	// Check if message and checkpoint are available
	if c.GetCheckpointMessage(sequenceNum) == nil || c.GetCheckpoint(sequenceNum) == nil {
		log.Warnf("[CheckpointManager] Checkpoint message or checkpoint for sequence number %d is not available", sequenceNum)
		return
	}

	// Compare the digest and add the checkpoint if the digest is the same
	if !bytes.Equal(c.GetCheckpointMessage(sequenceNum).Digest, c.GetCheckpoint(sequenceNum).Digest) {
		log.Warnf("[CheckpointManager] Digest mismatch for sequence number %d", sequenceNum)
		return
	}

	// Purge the logs and checkpoints for the sequence number
	c.Purge(sequenceNum)
}

// SendGetCheckpointRequest multicasts a request to get missing checkpoints
func (c *CheckpointManager) SendGetCheckpointRequest(sequenceNum int64) (*pb.Checkpoint, error) {
	getCheckpointRequest := &pb.GetCheckpointMessage{SequenceNum: sequenceNum, NodeID: c.id}

	// Logger: Add sent get checkpoint request message
	c.logger.AddSentGetCheckpointMessage(getCheckpointRequest)

	// Multicast get checkpoint request to all peers
	responseChan := make(chan *pb.Checkpoint, len(c.peers))
	wg := sync.WaitGroup{}
	for _, peer := range c.peers {
		wg.Add(1)
		go func(peer *models.Node) {
			defer wg.Done()
			checkpoint, err := (*peer.Client).GetCheckpoint(context.Background(), getCheckpointRequest)
			if err != nil || checkpoint == nil {
				return
			}
			responseChan <- checkpoint
		}(peer)
	}
	go func() {
		wg.Wait()
		close(responseChan)
	}()

	// Wait for response and return the checkpoint
	checkpoint, ok := <-responseChan
	if !ok {
		log.Warnf("[SendGetCheckpointRequest] Failed to get checkpoint")
		return nil, status.Errorf(codes.Unavailable, "failed to get checkpoint from leader")
	}

	// Logger: Add received get checkpoint message
	c.logger.AddReceivedCheckpoint(checkpoint)

	return checkpoint, nil
}

// Purge purges the logs, checkpoints, and checkpoint messages for a given sequence number
func (c *CheckpointManager) Purge(sequenceNum int64) {
	// Update last checkpointed sequence number
	c.state.UpdateLastCheckpointedSequenceNum(sequenceNum)
	log.Infof("[CheckpointManager] Updated last checkpointed sequence number to %d", sequenceNum)

	// Delete logs and checkpoints older than last checkpointed sequence number
	c.state.StateLog.PurgeBelowOrEqual(sequenceNum)
	for i := range c.checkpoints {
		if i < sequenceNum {
			c.DeleteCheckpoint(i)
		}
	}
	for i := range c.checkpointMessageLog {
		if i < sequenceNum {
			c.DeleteCheckpointMessage(i)
		}
	}
	log.Infof("[CheckpointManager] Purged logs, checkpoints, and checkpoint messages for sequence number %d", sequenceNum)
}
