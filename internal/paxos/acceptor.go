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
	executionTriggerCh chan int64
}

// AcceptRequestHandler handles the accept request for backup node
func (a *Acceptor) AcceptRequestHandler(acceptMessage *pb.AcceptMessage) (*pb.AcceptedMessage, error) {

	// Update state if higher ballot number
	if ballotNumberIsHigher(a.state.GetBallotNumber(), acceptMessage.B) {
		a.state.SetBallotNumber(acceptMessage.B)
	}

	// Create record if not exists
	created := a.state.StateLog.CreateRecordIfNotExists(acceptMessage.B, acceptMessage.SequenceNum, acceptMessage.Message)
	if created && !a.state.InForwardedRequestsLog(acceptMessage.Message) && !a.state.StateLog.IsExecuted(acceptMessage.SequenceNum) {
		a.timer.IncrementWaitCountOrStart()
	}

	// Update record state
	a.state.StateLog.SetAccepted(acceptMessage.SequenceNum)
	log.Infof("[Acceptor] Accepted %s", utils.AcceptMessageString(acceptMessage))

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

	// Create record if not exists
	created := a.state.StateLog.CreateRecordIfNotExists(commitMessage.B, commitMessage.SequenceNum, commitMessage.Message)
	if created && !a.state.InForwardedRequestsLog(commitMessage.Message) && !a.state.StateLog.IsExecuted(commitMessage.SequenceNum) {
		a.timer.IncrementWaitCountOrStart()
	}

	// Update record state
	a.state.StateLog.SetCommitted(commitMessage.SequenceNum)
	log.Infof("[Acceptor] Committed %s", utils.CommitMessageString(commitMessage))

	// Trigger execution if not executed
	if !a.state.StateLog.IsExecuted(commitMessage.SequenceNum) {
		a.executionTriggerCh <- commitMessage.SequenceNum
	}

	return &emptypb.Empty{}, nil
}

// CreateAcceptor creates a new acceptor
func CreateAcceptor(id string, state *ServerState, config *ServerConfig, peers map[string]*models.Node, timer *SafeTimer, executionTriggerCh chan int64) *Acceptor {
	return &Acceptor{
		id:                 id,
		state:              state,
		config:             config,
		peers:              peers,
		timer:              timer,
		executionTriggerCh: executionTriggerCh,
	}
}
