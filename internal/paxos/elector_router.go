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
	log.Infof("[PrepareRequestHandler] Waiting for response for ballot number %s", utils.BallotNumberString(prepareMessage.B))
	ok := <-responseCh
	if !ok {
		log.Warnf("[PrepareRequestHandler] Prepare request rejected for ballot number %s", utils.BallotNumberString(prepareMessage.B))
		return nil, errors.New("prepare request rejected")
	}

	// Update state
	// l.state.ResetForwardedRequestsLog()
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

func (l *LeaderElector) RunPreparePhase(ballotNumber *pb.BallotNumber) (bool, []*pb.AckMessage) {
	// Create prepare message
	prepareMessage := &pb.PrepareMessage{
		B: ballotNumber,
	}

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
				log.Warnf("[RunPreparePhase] Failed to send prepare request %s to %s: %s", utils.BallotNumberString(prepareMessage.B), peer.ID, err)
				return
			}

			// Logger: Add received ack message
			l.logger.AddReceivedAckMessage(ackMessage)

			log.Infof("[RunPreparePhase] Ack message from %s: %s", peer.ID, ackMessage.String())
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
	for {
		select {
		// case <-time.After(prepareTimeout):
		// 	log.Warnf("[RunPreparePhase] Context done for ballot number %s", utils.BallotNumberString(ballotNumber))
		// 	return false, nil
		case <-l.timer.ctx.Done():
			log.Warnf("[RunPreparePhase] Timer context done for ballot number %s at %d", utils.BallotNumberString(ballotNumber), time.Now().UnixMilli())
			return false, nil
		case ackMessage, ok := <-responseCh:
			if !ok {
				log.Warnf("[RunPreparePhase] Response channel closed for ballot number %s", utils.BallotNumberString(ballotNumber))
				return false, nil
			}
			ackMessages = append(ackMessages, ackMessage)
			accepted++
			if accepted >= l.config.F+1 {
				log.Infof("[RunPreparePhase] Accepted quorum for ballot number %s", utils.BallotNumberString(ballotNumber))
				return true, ackMessages
			}
		}
	}
}

// ElectionRoutine is the main routine for the leader election
func (l *LeaderElector) ElectionRoutine(ctx context.Context) {
electionLoop:
	for {
		select {
		case <-ctx.Done():
			log.Warnf("[ElectionRoutine] Context done")
			return
		case <-l.timer.TimeoutCh:
			log.Infof("[ElectionRoutine] Timer timeout for ballot number %s at %d", utils.BallotNumberString(l.state.GetBallotNumber()), time.Now().UnixMilli())
		}

		// Reset leader and forwarded requests log
		l.state.SetLeader("")
		l.state.ResetForwardedRequestsLog()

		// Handle prepare messages in queue
		highestBallotNumber, ok := l.PrepareQueueHandler(time.Now())
		if ok {
			// If there is a valid prepare message then continue election loop
			continue electionLoop
		}

		// Initiate Election: Update state and run prepare phase
		newBallotNumber := &pb.BallotNumber{
			N:      highestBallotNumber.N + 1,
			NodeID: l.id,
		}
		l.state.SetBallotNumber(newBallotNumber)
		elected, ackMessages := l.RunPreparePhase(newBallotNumber)
		if !elected {
			// If election is not successful then continue election loop
			continue electionLoop
		}

		// Election successful: Set leader and run new view phase
		l.state.SetLeader(l.id)
		log.Infof("[ElectionRoutine] New leader with promised ballot number %s", utils.BallotNumberString(l.state.GetBallotNumber()))
		go l.proposer.RunNewViewPhase(ackMessages)
	}
}
