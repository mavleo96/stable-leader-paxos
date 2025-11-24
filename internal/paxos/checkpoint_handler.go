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

// BackupCheckpointMessageHandler handles the backup checkpoint message
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

	c.checkpointPurgeRoutineCh <- sequenceNum
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
	c.state.SetLastCheckpointedSequenceNum(sequenceNum)
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

// CheckpointPurgeRoutine purges the logs and checkpoints for a given sequence number
func (c *CheckpointManager) CheckpointPurgeRoutine(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case sequenceNum := <-c.checkpointPurgeRoutineCh:
			// Check if checkpoint message is logged
			if c.GetCheckpointMessage(sequenceNum) == nil {
				log.Warnf("[CheckpointRoutine] Checkpoint message for sequence number %d is not logged", sequenceNum)
				continue
			}

			// Check if checkpoint is available
			if c.GetCheckpoint(sequenceNum) == nil {
				log.Warnf("[CheckpointRoutine] Checkpoint for sequence number %d is not available", sequenceNum)
				continue
			}

			// If both are available, then compare the digest and add the checkpoint if the digest is the same
			if !bytes.Equal(c.GetCheckpointMessage(sequenceNum).Digest, c.GetCheckpoint(sequenceNum).Digest) {
				log.Warnf("[CheckpointRoutine] Digest mismatch for sequence number %d", sequenceNum)
				continue
			}

			// Purge logs, checkpoints, and checkpoint messages
			c.Purge(sequenceNum)

			// // Update last checkpointed sequence number
			// lastCheckpointedSequenceNum := c.state.GetLastCheckpointedSequenceNum()
			// delta := sequenceNum - lastCheckpointedSequenceNum
			// c.state.SetLastCheckpointedSequenceNum(sequenceNum)
			// log.Infof("[CheckpointManager] Updated last checkpointed sequence number to %d", sequenceNum)

			// // Delete logs and checkpoints older than last checkpointed sequence number
			// for i := sequenceNum - delta + 1; i <= sequenceNum; i++ {
			// 	c.state.StateLog.Delete(i)
			// }
			// for i := range c.checkpoints {
			// 	if i < sequenceNum {
			// 		c.DeleteCheckpoint(i)
			// 	}
			// }
			// for i := range c.checkpointMessageLog {
			// 	if i < sequenceNum {
			// 		c.DeleteCheckpointMessage(i)
			// 	}
			// }
			// log.Infof("[CheckpointManager] Purged logs, checkpoints, and checkpoint messages for sequence number %d", sequenceNum)
		}
	}
}
