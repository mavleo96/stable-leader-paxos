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

// PrepareQueueHandler handles the queued prepare messages and returns the highest ballot number
// and if there was a valid prepare message
func (l *LeaderElector) PrepareQueueHandler(expiryTimeStamp time.Time) (*pb.BallotNumber, bool) {
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
			log.Infof("[PrepareQueueHandler] Prepare message %s is expired or not highest", utils.BallotNumberString(l.prepareMessageLog.msgLog[timestamp].B))
			l.prepareMessageLog.GetChannel(timestamp) <- false
			l.prepareMessageLog.DeletePrepareMessage(timestamp)
			continue
		}
		log.Infof("[PrepareQueueHandler] Prepare message %s is valid and highest", utils.BallotNumberString(l.prepareMessageLog.msgLog[timestamp].B))

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
		log.Infof("[PrepareQueueHandler] Promising highest ballot number %s", utils.BallotNumberString(currentHighestBallotNumber))
		return currentHighestBallotNumber, true
	}

	// If there is no valid timestamp, return false
	log.Infof("[PrepareQueueHandler] No valid prepare message found, highest ballot number is %s", utils.BallotNumberString(currentHighestBallotNumber))
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
	l.state.SetLeader(prepareMessage.B.NodeID)
	log.Infof("[PrepareRequestHandler] Promised ballot number %s", utils.BallotNumberString(l.state.GetBallotNumber()))

	// Return ack message with accepted log
	acceptedLog := l.state.StateLog.GetAcceptedLog()
	return &pb.AckMessage{
		B:         prepareMessage.B,
		AcceptLog: acceptedLog,
	}, nil
}

func (l *LeaderElector) RunPreparePhase(prepareMessage *pb.PrepareMessage) (bool, []*pb.AckMessage) {
	// Logger: Add sent prepare message
	l.logger.AddSentPrepareMessage(prepareMessage)

	// Multicast prepare message to all peers
	responseCh := make(chan *pb.AckMessage, len(l.peers))
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
			responseCh <- ackMessage
		}(peer)
	}
	go func() {
		wg.Wait()
		close(responseCh)
	}()

	// Wait for response and return true if all responses are true
	accepted := int64(1)
	ackMessages := make([]*pb.AckMessage, 0)
	for ackMessage := range responseCh {
		ackMessages = append(ackMessages, ackMessage)
		accepted++
		if accepted >= l.config.F+1 {
			return true, ackMessages
		}
	}
	return false, nil
}

// ElectionRoutine is the main routine for the leader election
func (l *LeaderElector) ElectionRoutine() {
electionLoop:
	for {
		<-l.timer.TimeoutCh
		// l.state.SetPreparePhase(true)
		log.Infof("[ElectionRoutine] Prepare phase started")
		l.state.SetLeader("")

		highestBallotNumber, ok := l.PrepareQueueHandler(time.Now())
		if ok {
			continue electionLoop
		}

		newBallotNumber := &pb.BallotNumber{
			N:      highestBallotNumber.N + 1,
			NodeID: l.id,
		}
		// Update state
		l.state.SetBallotNumber(newBallotNumber)

		prepareMessage := &pb.PrepareMessage{
			B: newBallotNumber,
		}
		elected, ackMessages := l.RunPreparePhase(prepareMessage)
		if !elected {
			continue electionLoop
		}

		l.state.SetLeader(l.id)
		log.Infof("[ElectionRoutine] New leader with promised ballot number %s", utils.BallotNumberString(l.state.GetBallotNumber()))

		l.proposer.RunNewViewPhase(ackMessages)
	}
}
