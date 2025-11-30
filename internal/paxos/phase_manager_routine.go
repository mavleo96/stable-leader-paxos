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
prepareLoop:
	for {
		log.Infof("[PhaseTimeoutRoutine] Prepare loop is waiting for messages at %d", time.Now().UnixMilli())
		select {
		// Server context cancelled
		case <-ctx.Done():
			log.Warnf("[PhaseTimeoutRoutine] Server context cancelled; stopping phase timeout routine")
			return

		// Receive Prepare Messages
		case prepareMessageEntry := <-pm.sendPrepareMessageCh:
			// Add prepare message to log
			pm.AddPrepareMessageToLog(prepareMessageEntry)
			log.Infof("[PhaseTimeoutRoutine] Added prepare message to log: %s", utils.LoggingString(prepareMessageEntry.PrepareMessage.B))

			// Check if timer has expired
			select {
			case <-pm.GetTimerCtx().Done():
				log.Warnf("[PhaseTimeoutRoutine] Timer already expired; initiating prepare queue handler on prepare message entry %s", utils.LoggingString(prepareMessageEntry.PrepareMessage.B))
				pm.PrepareQueueHandler(time.Now())
			default:
				log.Infof("[PhaseTimeoutRoutine] Timer not expired; continuing prepare loop")
				continue prepareLoop
			}

		// Receive Initiate Election Channel
		case initiateElectionRequest := <-pm.initiateElectionCh:
			log.Infof("[PhaseTimeoutRoutine] Initiate election channel received; initiating leader election handler")

			select {
			case <-pm.GetTimerCtx().Done():
				promised, elected := pm.leaderElectionHandler(initiateElectionRequest.ExpiredTime)
				initiateElectionRequest.SignalCh <- promised || elected
				close(initiateElectionRequest.SignalCh)
			default:
				log.Infof("[PhaseTimeoutRoutine] Timer not expired; continuing prepare loop")
				initiateElectionRequest.SignalCh <- true
				close(initiateElectionRequest.SignalCh)
				continue prepareLoop
			}

		// Receive Timer Expired
		case expiredTime := <-pm.timer.TimeoutCh:
			log.Infof("[PhaseTimeoutRoutine] Timer expired at %d", expiredTime.UnixMilli())
			pm.CancelTimerCtx()
			pm.CancelProposerCtx()

			// Handle election
			pm.leaderElectionHandler(expiredTime)
		}
	}
}

// leaderElectionHandler is the handler for the leader election; return bool1: promised someone; bool2: elected new leader
func (pm *PhaseManager) leaderElectionHandler(expiredTime time.Time) (bool, bool) {
	// Get current ballot number
	currentballotnumber := pm.state.GetBallotNumber()
	log.Infof("[PhaseTimeoutRoutine] Current ballot number: %s has expired at %d", utils.LoggingString(currentballotnumber), expiredTime.UnixMilli())

	// Reset leader and forwarded requests log
	pm.state.ResetLeader()
	pm.state.ResetForwardedRequestsLog()
	log.Infof("[PhaseTimeoutRoutine] Reset forwarded requests log of ballot number: %s", utils.LoggingString(currentballotnumber))

	// Handle the prepare message log
	promised, highestBallotNumber, _ := pm.PrepareQueueHandler(expiredTime)
	if promised {
		log.Infof("[PhaseTimeoutRoutine] Promised ballot number %s; continuing prepare loop", utils.LoggingString(highestBallotNumber))
		// continue prepareLoop
		return true, false
	}

	log.Infof("[PhaseTimeoutRoutine] Not promised; initiating prepare handler")

	// If not promised, initiate prepare handler
	newBallotNumber := &pb.BallotNumber{N: highestBallotNumber.N + 1, NodeID: pm.state.id}

	// Update state
	pm.state.SetBallotNumber(newBallotNumber)
	pm.ResetTimerCtx()

	elected := pm.initiatePrepareHandler(newBallotNumber)
	return false, elected
}
