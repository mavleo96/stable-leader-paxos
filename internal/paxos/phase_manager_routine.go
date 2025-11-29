package paxos

import (
	"context"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// PhaseTimeoutRoutine is the timeout routine for the phase manager
func (pm *PhaseManager) PhaseTimeoutRoutine(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case prepareMessageEntry := <-pm.sendPrepareMessageCh:
			pm.AddPrepareMessageToLog(prepareMessageEntry)
			if pm.GetTimerExpired() {
				pm.PrepareQueueHandler(time.Now())
			}

		case expiredTime := <-pm.timer.TimeoutCh:
			currentballotnumber := pm.state.GetBallotNumber()
			log.Infof("[PhaseTimeoutRoutine] Current ballot number: %s has expired at %d", utils.LoggingString(currentballotnumber), expiredTime.UnixMilli())

			// Reset leader and forwarded requests log
			pm.state.ResetLeader()
			pm.state.ResetForwardedRequestsLog()
			log.Infof("[PhaseTimeoutRoutine] Reset forwarded requests log of ballot number: %s", utils.LoggingString(currentballotnumber))

			// Reset timer context
			pm.timerCtx, pm.timerCancel = context.WithCancel(context.Background())

			// Handle the prepare message log
			promised, highestBallotNumber, _ := pm.PrepareQueueHandler(expiredTime)
			if promised {
				continue
			}

			// If not promised, initiate prepare handler
			newBallotNumber := &pb.BallotNumber{N: highestBallotNumber.N + 1, NodeID: pm.state.id}

			// Update state
			pm.state.SetBallotNumber(newBallotNumber)

			elected := pm.initiatePrepareHandler(newBallotNumber)
			if elected {
				log.Warnf("[PhaseTimeoutRoutine] Elected new leader for ballot number %s", utils.LoggingString(newBallotNumber))
				// pm.ResetTimerCtx()
				continue
			}

		}
	}
}
