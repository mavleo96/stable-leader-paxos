package paxos

import (
	"context"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	log "github.com/sirupsen/logrus"
)

func (e *Executor) ExecuteRouter(ctx context.Context) {
executeLoop:
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-e.executionTriggerCh:
			log.Infof("[Executor] Received execute signal for sequence number %d", s)
			sequenceNum := e.state.GetLastExecutedSequenceNum()
			maxSequenceNum := e.state.StateLog.MaxSequenceNum()
			if sequenceNum == maxSequenceNum {
				continue executeLoop
			}

		tryLoop:
			for i := sequenceNum + 1; i <= maxSequenceNum; i++ {
				if !e.state.StateLog.IsCommitted(i) {
					break tryLoop
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
				e.state.StateLog.SetResult(i, result)
				e.timer.DecrementWaitCountAndResetOrStopIfZero()
				log.Infof("[Executor] Executed %s", utils.TransactionRequestString(request))

				e.state.SetLastExecutedSequenceNum(i)

				// Send response to response channel if it exists
				if responseCh, ok := e.responseCh[i]; ok {
					log.Infof("[Executor] Sending response %d for sequence number %d", result, i)
					responseCh <- result
					e.CloseAndRemoveResponseChannel(i)
				}
			}
		}
	}
}
