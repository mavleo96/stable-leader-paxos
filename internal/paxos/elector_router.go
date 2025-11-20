package paxos

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// HandleQueuedPrepareMessagesAndDeleteExpired handles the queued prepare messages and returns the highest ballot number
// and if there was a valid prepare message
func (l *LeaderElector) HandleQueuedPrepareMessagesAndDeleteExpired(expiryTimeStamp time.Time) (*pb.BallotNumber, bool) {
	// Get timestamp keys of prepare messages
	timestampKeys := l.prepareMessageLog.GetLogKeys()

	// Initialize current highest ballot number and valid timestamp
	currentHighestBallotNumber := l.state.GetBallotNumber()
	var currentValidTimestamp time.Time

	// Iterate through timestamp keys
	for _, timestamp := range timestampKeys {
		// If prepare message is expired, delete it and send false to response channel
		// or if current highest ballot number is higher than the prepare message ballot number
		if expiryTimeStamp.Sub(timestamp) > prepareTimeout ||
			!BallotNumberIsHigher(currentHighestBallotNumber, l.prepareMessageLog.msgLog[timestamp].B) {
			delta := expiryTimeStamp.Sub(timestamp)
			cond1 := delta > prepareTimeout
			cond2 := !BallotNumberIsHigher(currentHighestBallotNumber, l.prepareMessageLog.msgLog[timestamp].B)
			log.Infof("[ElectionRoutine] Prepare message %s is expired by %s; request timeout is %s; cond1: %t; cond2: %t, current ballot number is %s; log ballot number is %s", utils.BallotNumberString(l.prepareMessageLog.msgLog[timestamp].B), delta, prepareTimeout, cond1, cond2, utils.BallotNumberString(currentHighestBallotNumber), utils.BallotNumberString(l.prepareMessageLog.msgLog[timestamp].B))
			l.prepareMessageLog.GetChannel(timestamp) <- false
			l.prepareMessageLog.DeletePrepareMessage(timestamp)
			continue
		}
		log.Infof("[ElectionRoutine] Prepare message %s is valid", utils.BallotNumberString(l.prepareMessageLog.msgLog[timestamp].B))

		// Update current valid timestamp and highest ballot number; close previous valid timestamp
		if currentValidTimestamp != (time.Time{}) {
			l.prepareMessageLog.GetChannel(currentValidTimestamp) <- false
			l.prepareMessageLog.DeletePrepareMessage(currentValidTimestamp)
		}
		currentValidTimestamp = timestamp
		currentHighestBallotNumber = l.prepareMessageLog.msgLog[timestamp].B
	}

	// If there is a valid timestamp, send true to response channel and return true
	if currentValidTimestamp != (time.Time{}) {
		l.prepareMessageLog.GetChannel(currentValidTimestamp) <- true
		l.prepareMessageLog.DeletePrepareMessage(currentValidTimestamp)
		log.Infof("[ElectionRoutine] Promising highest ballot number %s", utils.BallotNumberString(currentHighestBallotNumber))
		return currentHighestBallotNumber, true
	}

	// If there is no valid timestamp, return false
	log.Infof("[ElectionRoutine] No valid prepare message found, highest ballot number is %s", utils.BallotNumberString(currentHighestBallotNumber))
	return currentHighestBallotNumber, false
}

// PrepareRequestHandler handles the prepare request for backup node
func (l *LeaderElector) PrepareRequestHandler(prepareMessage *pb.PrepareMessage) (*pb.AckMessage, error) {

	// Log the prepare request
	responseCh := make(chan bool)
	l.prepareMessageLog.AddPrepareMessage(prepareMessage, responseCh)

	// Wait for response and return error if rejected
	ok := <-responseCh
	if !ok {
		log.Warnf("[PrepareRequestHandler] Prepare request rejected for ballot number %s", utils.BallotNumberString(prepareMessage.B))
		return nil, errors.New("prepare request rejected")
	}

	// Update state
	l.state.ResetForwardedRequestsLog()
	l.state.SetBallotNumber(prepareMessage.B)
	l.state.SetPreparePhase(false)
	log.Infof("[PrepareRequestHandler] Promised ballot number %s", utils.BallotNumberString(l.state.GetBallotNumber()))

	// Return ack message with accepted log
	acceptedLog := l.state.StateLog.GetAcceptedLog()
	return &pb.AckMessage{
		B:         prepareMessage.B,
		AcceptLog: acceptedLog,
	}, nil
}

func (l *LeaderElector) RunPreparePhase(prepareMessage *pb.PrepareMessage) bool {
	// Multicast prepare message to all peers
	responseCh := make(chan bool, len(l.peers))
	wg := sync.WaitGroup{}
	for _, peer := range l.peers {
		wg.Add(1)
		go func(peer *models.Node) {
			defer wg.Done()
			ackMessage, err := (*peer.Client).PrepareRequest(context.Background(), prepareMessage)
			if err != nil || ackMessage == nil {
				log.Warnf("Failed to send prepare request to %s: %s", peer.ID, err)
				return
			}
			log.Infof("Ack message from %s: %s", peer.ID, ackMessage.String())
			responseCh <- true
		}(peer)
	}
	go func() {
		wg.Wait()
		close(responseCh)
	}()

	// Wait for response and return true if all responses are true
	accepted := int64(1)
	for a := range responseCh {
		if a {
			accepted++
		}
		if accepted >= l.config.F+1 {
			return true
		}
	}
	return false
}

// ElectionRoutine is the main routine for the leader election
func (l *LeaderElector) ElectionRoutine() {
	for {
		<-l.timer.TimeoutCh
		l.state.SetPreparePhase(true)
		log.Infof("[ElectionRoutine] Prepare phase started")

		highestBallotNumber, ok := l.HandleQueuedPrepareMessagesAndDeleteExpired(time.Now())
		if ok {
			continue
		}

		newBallotNumber := &pb.BallotNumber{
			N:      highestBallotNumber.N + 1,
			NodeID: l.id,
		}
		prepareMessage := &pb.PrepareMessage{
			B: newBallotNumber,
		}
		accepted := l.RunPreparePhase(prepareMessage)
		if accepted {
			// Update state
			l.state.SetBallotNumber(newBallotNumber)
			l.state.SetPreparePhase(false)
			log.Infof("[ElectionRoutine] Promised ballot number %s", utils.BallotNumberString(l.state.GetBallotNumber()))
			continue
		}
	}
}
