package paxos

import (
	"context"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// ExecuteRouter is the main routine for the executor
func (e *Executor) ExecuteRouter(ctx context.Context) {
	// pendingExecuteRequests := make([]ExecuteRequest, 0)
executeLoop:
	for {
		select {
		case <-ctx.Done():
			return
		case executeRequest := <-e.executionTriggerCh:
			log.Infof("[Executor] Received execute request for sequence number %d", executeRequest.SequenceNum)

			// Enqueue execute request
			e.enqueue(executeRequest.SequenceNum, executeRequest)

			// If request is already executed, send signal to requestor
			if executeRequest.SequenceNum <= e.state.GetLastCheckpointedSequenceNum() ||
				e.state.StateLog.IsExecuted(executeRequest.SequenceNum) {
				// TODO: this is a debug value to check for race condition
				executeRequest.ResultCh <- -1000
				close(executeRequest.ResultCh)
				continue executeLoop
			}

			// Try to execute the transaction
			lastExecutedSequenceNum := e.state.GetLastExecutedSequenceNum()
			maxSequenceNum := e.state.MaxSequenceNum()
		tryLoop:
			for i := lastExecutedSequenceNum + 1; i <= maxSequenceNum; i++ {
				if !e.state.StateLog.IsCommitted(i) {
					log.Infof("[Executor] Sequence number %d is not committed", i)
					continue executeLoop
				}
				if e.state.StateLog.IsExecuted(i) {
					log.Fatalf("[Executor] Sequence number %d was executed but state maxexecuted sequence number is %d", i, e.state.GetLastExecutedSequenceNum())
					continue tryLoop
				}

				// Execute transaction
				request := e.state.StateLog.GetRequest(i)
				var result int64
				var err error
				if !proto.Equal(request, NoOperation) {
					var success bool
					success, err = e.db.UpdateDB(request.Transaction)
					if err != nil {
						log.Warn(err)
					}
					result = utils.BoolToInt64(success)
				}

				// Add to executed log
				e.state.StateLog.SetExecuted(i)
				e.state.DedupTable.UpdateLastResult(request.Sender, request.Timestamp, result)
				e.timer.DecrementWaitCountAndResetOrStopIfZero()
				log.Infof("[Executor] Executed sequence number %d, %s", i, utils.LoggingString(request))

				e.state.SetLastExecutedSequenceNum(i)

				// Create checkpoint and save to checkpoint manager
				if i%e.config.K == 0 {
					dbState, err := e.db.GetDBState()
					if err != nil {
						log.Warn(err)
					}
					log.Infof("[Executor] Creating checkpoint for sequence number %d", i)
					e.checkpointer.AddCheckpoint(i, dbState)
					e.checkpointer.GetCheckpointPurgeRoutineCh() <- i
				}

				// Dequeue execute requests
				log.Infof("[Executor] Dequeuing execute requests and sending signals for sequence number %d", i)
				executeRequests := e.dequeue(i)
				for _, executeRequest := range executeRequests {
					executeRequest.ResultCh <- result
					close(executeRequest.ResultCh)
				}
			}

		case sequenceNum := <-e.installCheckpointCh:
			// Get checkpoint
			checkpoint := e.checkpointer.GetCheckpoint(sequenceNum)
			if checkpoint == nil {
				log.Warnf("[Executor] Checkpoint for sequence number %d is not available", sequenceNum)
				continue executeLoop
			}

			// If executed sequence number is greater than or equal to checkpoint sequence number, skip
			if sequenceNum <= e.state.GetLastExecutedSequenceNum() {
				log.Infof("[Executor] Checkpoint for sequence number %d is already installed", sequenceNum)
				continue executeLoop
			}

			// Install checkpoint
			snapshot := checkpoint.Snapshot
			for clientID, balance := range snapshot {
				err := e.db.SetBalance(clientID, balance)
				if err != nil {
					log.Fatalf("[Executor] Failed to set balance for client %s: %v", clientID, err)
				}
			}
			log.Infof("[Executor] Installed checkpoint for sequence number %d", sequenceNum)

			// Update state
			e.state.SetLastExecutedSequenceNum(sequenceNum)

			// Purge logs, checkpoints, and checkpoint messages
			e.checkpointer.Purge(sequenceNum)

			// // Trigger checkpoint purge routine
			// e.checkpointer.GetCheckpointPurgeRoutineCh() <- sequenceNum
		}
	}
}
