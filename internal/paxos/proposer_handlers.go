package paxos

import (
	"errors"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// HandleTransactionRequest handles the transaction request and returns the sequence number
func (p *Proposer) HandleTransactionRequest(req *pb.TransactionRequest) error {
	// Get current ballot number
	currentBallotNumber := p.state.GetBallotNumber()

	// Assign a sequence number and create a record if it doesn't exist
	sequenceNum, _ := p.state.AssignSequenceNumberAndCreateRecord(currentBallotNumber, req)
	log.Infof("[Proposer] Assigned sequence number %d for request %s", sequenceNum, utils.LoggingString(req))

	pendingCount := p.state.StateLog.GetPendingCount()
	if pendingCount > 0 {
		p.phaseManager.timer.StartIfNotRunning()
	}

	// Note: We are accepting the request even if the timer is expired; but this maybe handled in transfer.go where this function is called
	select {
	case <-p.phaseManager.GetProposerCtx().Done():
		return errors.New("not leader/proposer")
	default:
	}

	// Run accept phase and set accepted flag
	if !p.state.StateLog.IsCommitted(sequenceNum) {
		acceptedCh := make(chan error, 1)
		go func() {
			accepted, err := p.RunAcceptPhase(sequenceNum, currentBallotNumber, p.state.StateLog.GetRequest(sequenceNum))
			if err != nil {
				acceptedCh <- err
			}
			if !accepted {
				acceptedCh <- errors.New("insufficient quorum")
			}
			acceptedCh <- nil
		}()

		// Wait for accepted phase to complete
		select {
		case err := <-acceptedCh:
			if err != nil {
				return err
			}
		case <-p.phaseManager.GetProposerCtx().Done():
			log.Warnf("[Proposer] Accept phase cancelled for request %s while waiting for accepted messages", utils.LoggingString(req))
			return errors.New("not leader/proposer")
		}
	}

	// Run commit phase and set committed flag
	err := p.RunCommitPhase(sequenceNum, currentBallotNumber, p.state.StateLog.GetRequest(sequenceNum))

	// Send checkpoint message if sequence number is a multiple of k and purge
	if err == nil && sequenceNum%p.config.K == 0 {
		// Check if checkpoint is available
		if p.checkpointer.GetCheckpoint(sequenceNum) != nil {
			digest := p.checkpointer.GetCheckpoint(sequenceNum).Digest
			p.SendCheckpointMessage(sequenceNum, digest)
			p.checkpointer.Purge(sequenceNum)
		}
	}

	return err
}

// RunAcceptPhase sends an accept request to all peers and returns the response from each peer
func (p *Proposer) RunAcceptPhase(sequenceNum int64, ballotNumber *pb.BallotNumber, req *pb.TransactionRequest) (bool, error) {
	// Set accepted flag and create accept message
	p.state.StateLog.SetAccepted(sequenceNum)
	acceptMessage := &pb.AcceptMessage{
		B:           ballotNumber,
		SequenceNum: sequenceNum,
		Message:     req,
	}

	// Multicast accept request to all peers
	responseCh := make(chan bool, len(p.peers))
	go p.SendAcceptMessage(acceptMessage, responseCh)

	// Wait for responses and check if quorum of accepts is reached
	acceptedCount := int64(1)
	for a := range responseCh {
		if a {
			acceptedCount++
		}
		if acceptedCount >= p.config.F+1 {
			log.Infof("[Proposer] Accept phase successful for request %s", utils.LoggingString(req))
			return true, nil
		}
	}
	log.Warnf("[Proposer] Accept phase failed for request %s: insufficient quorum", utils.LoggingString(req))
	return false, nil
}

// RunCommitPhase commits the transaction request
func (p *Proposer) RunCommitPhase(sequenceNum int64, ballotNumber *pb.BallotNumber, req *pb.TransactionRequest) error {
	// Set committed flag and create commit message
	p.state.StateLog.SetCommitted(sequenceNum)
	commitMessage := &pb.CommitMessage{
		B:           ballotNumber,
		SequenceNum: sequenceNum,
		Message:     req,
	}

	// Broadcast commit request to all peers
	go p.BroadcastCommitMessage(commitMessage)

	// If not executed, trigger execution
	if !p.state.StateLog.IsExecuted(sequenceNum) {
		resultCh := make(chan int64, 1)
		p.executionTriggerCh <- ExecuteRequest{
			SequenceNum: sequenceNum,
			ResultCh:    resultCh,
		}
		<-resultCh
	}

	return nil
}

// RunNewViewPhase runs the new view phase
func (p *Proposer) RunNewViewPhase(ballotNumber *pb.BallotNumber, checkpointedSequenceNum int64, acceptMessages []*pb.AcceptMessage) {
	// Create new view message
	newViewMessage := &pb.NewViewMessage{
		B:           ballotNumber,
		SequenceNum: checkpointedSequenceNum,
		AcceptLog:   acceptMessages,
	}
	log.Infof("[RunNewViewPhase] Sending new view request to all peers for ballot number %s", utils.LoggingString(ballotNumber))

	// Multicast new view request to all peers
	responseCh := make(chan *pb.AcceptedMessage, 100)
	go p.SendNewViewMessage(newViewMessage, responseCh)

	// Wait for responses and check if quorum of accepts is reached
	acceptedCountMap := make(map[int64]int64, 100)
	for {
		// Accept messages are processed until context is cancelled or response channel is closed
		select {
		case <-p.phaseManager.GetProposerCtx().Done():
			log.Infof("[RunNewViewPhase] New view phase cancelled for ballot number %s", utils.LoggingString(ballotNumber))
			return
		case acceptedMessage, ok := <-responseCh:
			if !ok {
				log.Infof("[RunNewViewPhase] New view phase completed for ballot number %s", utils.LoggingString(ballotNumber))
				return
			}
			sequenceNum := acceptedMessage.SequenceNum
			acceptedCountMap[sequenceNum]++
			if acceptedCountMap[sequenceNum] == p.config.F {
				go p.RunCommitPhase(sequenceNum, ballotNumber, acceptedMessage.Message)
			}
		}
	}
}
