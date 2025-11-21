package paxos

import (
	"context"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	log "github.com/sirupsen/logrus"
)

// ExecuteRouter is the main routine for the executor
func (e *Executor) ExecuteRouter(ctx context.Context) {
executeLoop:
	for {
		select {
		case <-ctx.Done():
			return
		case executeRequest := <-e.executionTriggerCh:
			log.Infof("[Executor] Received execute signal for sequence number %d", executeRequest.SequenceNum)

			// If request is already executed, send signal to requestor
			if e.state.StateLog.IsExecuted(executeRequest.SequenceNum) {
				executeRequest.SignalCh <- true
				close(executeRequest.SignalCh)
				continue executeLoop
			}

			// Try to execute the transaction
			lastExecutedSequenceNum := e.state.GetLastExecutedSequenceNum()
		tryLoop:
			for i := lastExecutedSequenceNum + 1; i <= executeRequest.SequenceNum; i++ {
				if !e.state.StateLog.IsCommitted(i) {
					// Requeue execution request
					e.executionTriggerCh <- executeRequest
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
				if request != NoOperation {
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
				log.Infof("[Executor] Executed %s", utils.TransactionRequestString(request))

				e.state.SetLastExecutedSequenceNum(i)

				// // Create checkpoint and save to checkpoint manager
				// if i%e.config.K == 0 {
				// 	dbState, err := e.db.GetDBState()
				// 	if err != nil {
				// 		log.Warn(err)
				// 	}
				// 	log.Infof("[Executor] Creating checkpoint for sequence number %d", i)
				// 	e.checkpointer.AddCheckpoint(i, dbState)
				// }

			}

			executeRequest.SignalCh <- true
			close(executeRequest.SignalCh)
		}
	}
}
