package paxos

import (
	"errors"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// PrepareQueueHandler handles the prepare messages queued on timeout
func (pm *PhaseManager) PrepareQueueHandler(expiredTime time.Time) (bool, *pb.BallotNumber, error) {
	// Get highest valid prepare message in log and if the message is valid
	highestBallotNumber, valid := pm.GetHighestValidPrepareMessageInLog(expiredTime)
	log.Infof("[PrepareQueueHandler] Highest valid prepare message in log: %s, valid: %t", utils.LoggingString(highestBallotNumber), valid)

	// Loop through prepare messages
	for _, prepareMessageEntry := range pm.GetPrepareMessageLog() {
		// If ballot number is not the same, respond false on channel
		if compareBallotNumbers(prepareMessageEntry.PrepareMessage.B, highestBallotNumber) != 0 {
			// Respond false on channel
			if prepareMessageEntry.ResponseCh != nil {
				log.Infof("[PrepareQueueHandler] Responding false on channel for ballot number %s since it is not the highest ballot number", utils.LoggingString(prepareMessageEntry.PrepareMessage.B))
				prepareMessageEntry.ResponseCh <- false
				close(prepareMessageEntry.ResponseCh)
				prepareMessageEntry.ResponseCh = nil
			}
		} else if !valid {
			// If prepare message is expired, respond false on channel
			// Respond false on channel
			if prepareMessageEntry.ResponseCh != nil {
				log.Infof("[PrepareQueueHandler] Responding false on channel for ballot number %s since it is expired", utils.LoggingString(prepareMessageEntry.PrepareMessage.B))
				prepareMessageEntry.ResponseCh <- false
				close(prepareMessageEntry.ResponseCh)
				prepareMessageEntry.ResponseCh = nil
			}
		} else {
			// If prepare message is valid then update state
			log.Infof("[PrepareQueueHandler] Updating state for ballot number %s", utils.LoggingString(prepareMessageEntry.PrepareMessage.B))
			pm.state.SetBallotNumber(prepareMessageEntry.PrepareMessage.B)
			pm.state.SetLeader(prepareMessageEntry.PrepareMessage.B.NodeID)
			pm.state.ResetForwardedRequestsLog()
			pm.ResetTimerCtx()

			// Respond true on channel
			if prepareMessageEntry.ResponseCh != nil {
				log.Infof("[PrepareQueueHandler] Responding true on channel for ballot number %s since it is the highest ballot number", utils.LoggingString(prepareMessageEntry.PrepareMessage.B))
				prepareMessageEntry.ResponseCh <- true
				close(prepareMessageEntry.ResponseCh)
				prepareMessageEntry.ResponseCh = nil
			}
		}
	}

	return valid, highestBallotNumber, nil
}

// PrepareRequestHandler handles the prepare request; waits for response and returns ack message if accepted
func (pm *PhaseManager) PrepareRequestHandler(prepareMessage *pb.PrepareMessage) (*pb.AckMessage, error) {
	// Log the prepare request
	responseCh := make(chan bool, 1)
	prepareMessageEntry := &PrepareMessageEntry{
		PrepareMessage: prepareMessage,
		ResponseCh:     responseCh,
		Timestamp:      time.Now(),
	}
	pm.GetSendPrepareMessageCh() <- prepareMessageEntry

	// Note: server state is already updated in PrepareQueueHandler
	valid := <-responseCh
	if !valid {
		log.Warnf("[PrepareRequestHandler] Prepare request rejected for ballot number %s", utils.LoggingString(prepareMessage.B))
		return nil, errors.New("prepare request rejected for ballot number")
	}

	log.Infof("[PrepareRequestHandler] Promised ballot number %s", utils.LoggingString(pm.state.GetBallotNumber()))

	// Return ack message with accepted log
	acceptedLog := pm.state.StateLog.GetAcceptedLog()
	sequenceNum := pm.state.GetLastCheckpointedSequenceNum()
	return &pb.AckMessage{
		B:           prepareMessage.B,
		SequenceNum: sequenceNum,
		AcceptLog:   acceptedLog,
	}, nil
}

// InitiatePrepareHandler initiates the prepare phase
func (p *Proposer) InitiatePrepareHandler(ballotNumber *pb.BallotNumber) bool {
	// Create prepare message
	prepareMessage := &pb.PrepareMessage{B: ballotNumber}

	// Multicast prepare message to all peers
	responseCh := make(chan *pb.AckMessage, len(p.peers))
	go p.SendPrepareMessage(prepareMessage, responseCh)

	// Wait for response and return true if all responses are true
	accepted := int64(1)
	// Create ack message entry
	ackMessageEntry := &pb.AckMessage{
		B:           ballotNumber,
		SequenceNum: p.state.GetLastCheckpointedSequenceNum(),
		AcceptLog:   p.state.StateLog.GetAcceptedLog(),
	}
	ackMessages := make([]*pb.AckMessage, 0)
	ackMessages = append(ackMessages, ackMessageEntry)
collectLoop:
	for {
		select {
		case <-time.After(prepareTimeout):
			log.Warnf("[InitiatePrepareHandler] Timer context done for ballot number %s at %d", utils.LoggingString(ballotNumber), time.Now().UnixMilli())
			p.phaseManager.CancelTimerCtx()
			return false
		case ackMessage, ok := <-responseCh:
			if !ok {
				log.Warnf("[InitiatePrepareHandler] Response channel closed for ballot number %s", utils.LoggingString(ballotNumber))
				p.phaseManager.CancelTimerCtx()
				return false
			}
			ackMessages = append(ackMessages, ackMessage)
			accepted++
			if accepted >= p.config.F+1 {
				log.Infof("[InitiatePrepareHandler] Accepted quorum for ballot number %s", utils.LoggingString(ballotNumber))
				break collectLoop
			}
		}
	}

	// Aggregate ack messages
	checkpointSequenceNum, acceptMessages := aggregateAckMessages(ballotNumber, ackMessages)

	// Handle checkpoint
	if checkpointSequenceNum > p.state.GetLastCheckpointedSequenceNum() {
		// Check if checkpoint is available
		checkpoint := p.checkpointer.GetCheckpoint(checkpointSequenceNum)
		if checkpoint == nil {
			log.Warnf("[InitiatePrepareHandler] Checkpoint for sequence number %d is not available", checkpointSequenceNum)
			checkpoint, err := p.checkpointer.SendGetCheckpointRequest(checkpointSequenceNum)
			if err != nil {
				log.Warnf("[InitiatePrepareHandler] Failed to get checkpoint for sequence number %d: %v", checkpointSequenceNum, err)
				return false
			}
			p.checkpointer.AddCheckpoint(checkpointSequenceNum, checkpoint.Snapshot)
		}
		signalCh := make(chan struct{}, 1)
		checkpointInstallRequest := CheckpointInstallRequest{
			SequenceNum: checkpointSequenceNum,
			SignalCh:    signalCh,
		}
		p.installCheckpointCh <- checkpointInstallRequest
		<-signalCh
		p.checkpointer.Purge(checkpointSequenceNum)
	}

	// Update state with new accept messages
	for _, acceptMessage := range acceptMessages {
		p.state.StateLog.CreateRecordIfNotExists(ballotNumber, acceptMessage.SequenceNum, acceptMessage.Message)
		p.state.StateLog.SetAccepted(acceptMessage.SequenceNum)
	}
	// Set leader and reset proposer contexts
	p.state.SetLeader(p.id)
	p.phaseManager.ResetProposerCtx()

	go p.RunNewViewPhase(ballotNumber, checkpointSequenceNum, acceptMessages)
	log.Infof("[InitiatePrepareHandler] New leader with promised ballot number %s at %d", utils.LoggingString(ballotNumber), time.Now().UnixMilli())
	return true
}

// HandleBallotNumber validates and updates the ballot number when a higher ballot is received
func (pm *PhaseManager) HandleBallotNumber(ballotNumber *pb.BallotNumber) bool {
	// Compare ballot numbers; return true if ballot is valid and false if not
	switch compareBallotNumbers(ballotNumber, pm.state.GetBallotNumber()) {
	case 1:
		// Update state and stop timer
		pm.state.SetBallotNumber(ballotNumber)
		pm.state.SetLeader(ballotNumber.NodeID)
		pm.state.ResetForwardedRequestsLog()
		pm.timer.Stop()
		pm.ResetTimerCtx()
		// pm.ResetProposerCtx()
		pm.CancelProposerCtx()

		log.Infof("[HandleBallotNumber] Changed ballot number to %s", utils.LoggingString(ballotNumber))

		return true
	case 0:
		return true
	case -1:
		return false
	default:
		log.Panicf("Invalid ballot number (%s vs %s) comparison: %d", utils.LoggingString(pm.state.GetBallotNumber()), utils.LoggingString(ballotNumber), compareBallotNumbers(pm.state.GetBallotNumber(), ballotNumber))
		return false
	}
}
