package paxos

import (
	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Acceptor structure handles acceptor logic for backup node
type Acceptor struct {
	id     string
	state  *ServerState
	config *ServerConfig
	peers  map[string]*models.Node

	// Timer instance
	timer *SafeTimer

	// Channels
	executionTriggerCh chan ExecuteRequest
}

// AcceptRequestHandler handles the accept request for backup node
func (a *Acceptor) AcceptRequestHandler(acceptMessage *pb.AcceptMessage) (*pb.AcceptedMessage, error) {

	// Update state if higher ballot number
	if ballotNumberIsHigher(a.state.GetBallotNumber(), acceptMessage.B) {
		a.state.SetBallotNumber(acceptMessage.B)
	}

	// Ignore if below checkpointed sequence number
	if acceptMessage.SequenceNum <= a.state.GetLastCheckpointedSequenceNum() {
		log.Infof("[Acceptor] Ignored accept request for sequence number %d since it is below checkpointed sequence number %d", acceptMessage.SequenceNum, a.state.GetLastCheckpointedSequenceNum())
		// return nil, status.Errorf(codes.FailedPrecondition, "sequence number is below checkpointed sequence number")
		return nil, nil
	}

	// Create record if not exists
	created := a.state.StateLog.CreateRecordIfNotExists(acceptMessage.B, acceptMessage.SequenceNum, acceptMessage.Message)
	if created && !a.state.InForwardedRequestsLog(acceptMessage.Message) && !a.state.StateLog.IsExecuted(acceptMessage.SequenceNum) {
		a.timer.IncrementWaitCountOrStart()
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

	// Update state if higher ballot number
	if ballotNumberIsHigher(a.state.GetBallotNumber(), commitMessage.B) {
		a.state.SetBallotNumber(commitMessage.B)
	}

	// Ignore if below checkpointed sequence number
	if commitMessage.SequenceNum <= a.state.GetLastCheckpointedSequenceNum() {
		log.Infof("[Acceptor] Ignored commit request for sequence number %d since it is below checkpointed sequence number %d", commitMessage.SequenceNum, a.state.GetLastCheckpointedSequenceNum())
		// return &emptypb.Empty{}, status.Errorf(codes.FailedPrecondition, "sequence number is below checkpointed sequence number")
		return &emptypb.Empty{}, nil
	}

	// Create record if not exists
	created := a.state.StateLog.CreateRecordIfNotExists(commitMessage.B, commitMessage.SequenceNum, commitMessage.Message)
	if created && !a.state.InForwardedRequestsLog(commitMessage.Message) && !a.state.StateLog.IsExecuted(commitMessage.SequenceNum) {
		a.timer.IncrementWaitCountOrStart()
	}

	// Update record state
	a.state.StateLog.SetCommitted(commitMessage.SequenceNum)
	log.Infof("[Acceptor] Committed %s", utils.LoggingString(commitMessage))

	// Trigger execution if not executed
	if !a.state.StateLog.IsExecuted(commitMessage.SequenceNum) {
		signalCh := make(chan bool, 1)
		a.executionTriggerCh <- ExecuteRequest{
			SequenceNum: commitMessage.SequenceNum,
			SignalCh:    signalCh,
		}
		<-signalCh
	}

	return &emptypb.Empty{}, nil
}

// CreateAcceptor creates a new acceptor
func CreateAcceptor(id string, state *ServerState, config *ServerConfig, peers map[string]*models.Node, timer *SafeTimer, executionTriggerCh chan ExecuteRequest) *Acceptor {
	return &Acceptor{
		id:                 id,
		state:              state,
		config:             config,
		peers:              peers,
		timer:              timer,
		executionTriggerCh: executionTriggerCh,
	}
}
