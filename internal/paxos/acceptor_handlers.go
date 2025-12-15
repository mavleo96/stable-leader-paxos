package paxos

import (
	"errors"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
)

// AcceptRequestHandler handles the accept request for backup node
func (a *Acceptor) AcceptRequestHandler(acceptMessage *pb.AcceptMessage) (*pb.AcceptedMessage, error) {

	// Check and update phase
	if !a.phaseManager.HandleBallotNumber(acceptMessage.B) {
		log.Warnf("[Acceptor] Ballot number %s is lower than promised ballot number %s", utils.LoggingString(acceptMessage.B), utils.LoggingString(a.state.GetBallotNumber()))
		return nil, errors.New("ballot number is lower than promised ballot number")
	}

	// Ignore if below checkpointed sequence number
	if acceptMessage.SequenceNum <= a.state.GetLastCheckpointedSequenceNum() {
		log.Infof("[Acceptor] Ignored accept request for sequence number %d since it is below checkpointed sequence number %d", acceptMessage.SequenceNum, a.state.GetLastCheckpointedSequenceNum())
		// return nil, status.Errorf(codes.FailedPrecondition, "sequence number is below checkpointed sequence number")
		return nil, nil
	}

	a.state.StateLog.CreateRecordIfNotExists(acceptMessage.B, acceptMessage.SequenceNum, acceptMessage.Message)

	// Start timer if there are pending transactions
	pendingCount := a.state.StateLog.GetPendingCount()
	if pendingCount > 0 {
		a.phaseManager.timer.StartIfNotRunning()
	}

	// Update record state
	a.state.StateLog.SetAccepted(acceptMessage.SequenceNum)
	log.Infof("[Acceptor] Accepted %s", utils.LoggingString(acceptMessage))

	return &pb.AcceptedMessage{
		B:           acceptMessage.B,
		SequenceNum: acceptMessage.SequenceNum,
		Message:     acceptMessage.Message,
		NodeID:      a.id,
	}, nil
}

// CommitRequestHandler handles the commit request for backup node
func (a *Acceptor) CommitRequestHandler(commitMessage *pb.CommitMessage) (*emptypb.Empty, error) {

	// Check and update phase
	if !a.phaseManager.HandleBallotNumber(commitMessage.B) {
		log.Warnf("[Acceptor] Ballot number %s is lower than promised ballot number %s", utils.LoggingString(commitMessage.B), utils.LoggingString(a.state.GetBallotNumber()))
		return nil, errors.New("ballot number is lower than promised ballot number")
	}

	// Ignore if below checkpointed sequence number
	if commitMessage.SequenceNum <= a.state.GetLastCheckpointedSequenceNum() {
		log.Infof("[Acceptor] Ignored commit request for sequence number %d since it is below checkpointed sequence number %d", commitMessage.SequenceNum, a.state.GetLastCheckpointedSequenceNum())
		// return &emptypb.Empty{}, status.Errorf(codes.FailedPrecondition, "sequence number is below checkpointed sequence number")
		return &emptypb.Empty{}, nil
	}

	a.state.StateLog.CreateRecordIfNotExists(commitMessage.B, commitMessage.SequenceNum, commitMessage.Message)

	// Start timer if there are pending transactions
	pendingCount := a.state.StateLog.GetPendingCount()
	if pendingCount > 0 {
		a.phaseManager.timer.StartIfNotRunning()
	}

	// Update record state
	a.state.StateLog.SetCommitted(commitMessage.SequenceNum)
	log.Infof("[Acceptor] Committed %s", utils.LoggingString(commitMessage))

	// Trigger execution if not executed
	if !a.state.StateLog.IsExecuted(commitMessage.SequenceNum) {
		resultCh := make(chan int64, 1)
		a.executionTriggerCh <- ExecuteRequest{
			SequenceNum: commitMessage.SequenceNum,
			ResultCh:    resultCh,
		}
		<-resultCh
	}

	// Try checkpoint handler if sequence number is a multiple of k
	if commitMessage.SequenceNum%a.config.K == 0 {
		a.checkpointer.BackupTryCheckpointHandler(commitMessage.SequenceNum)
	}

	return &emptypb.Empty{}, nil
}

// NewViewRequestHandler handles the new view request for backup node
func (a *Acceptor) NewViewRequestHandler(newViewMessage *pb.NewViewMessage) error {
	// Check and update phase
	if a.state.StateLog.GetPendingCount() > 0 {
		a.phaseManager.timer.Reset()
	} else {
		a.phaseManager.timer.StopIfRunning()
	}

	if !a.phaseManager.HandleBallotNumber(newViewMessage.B) {
		log.Warnf("[Acceptor] Ballot number %s is lower than promised ballot number %s", utils.LoggingString(newViewMessage.B), utils.LoggingString(a.state.GetBallotNumber()))
		return errors.New("ballot number is lower than promised ballot number")
	}

	return nil
}
