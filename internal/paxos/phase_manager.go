package paxos

import (
	"context"
	"sync"
	"time"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

// PhaseManager is the manager for the phases of the paxos algorithm
type PhaseManager struct {
	mutex sync.Mutex
	id    string

	// Components
	timer *SafeTimer
	state *ServerState

	// Timer context
	// Note: this context is owned by phase manager and not the timer
	// because we have have to stop/reset the timer without cancelling the context
	// Ex: When we recieve we a request with higher ballot is handled by acceptor module
	timerCtx    context.Context
	timerCancel context.CancelFunc

	// When a higher ballot number is received and if proposer, we need to stop proposer handlers without timing out
	// Because timing out triggers prepare handlers
	proposerCtx    context.Context
	proposerCancel context.CancelFunc

	// Prepare message log
	prepareMessageLog    []*PrepareMessageEntry
	sendPrepareMessageCh chan *PrepareMessageEntry

	// Function pointers
	initiatePrepareHandler func(ballotNumber *pb.BallotNumber) bool
}

// PrepareMessageEntry is the entry for a prepare message
type PrepareMessageEntry struct {
	PrepareMessage *pb.PrepareMessage
	ResponseCh     chan bool
	Timestamp      time.Time
}

// GetSendPrepareMessageCh returns the channel to send prepare messages to the phase manager
func (pm *PhaseManager) GetSendPrepareMessageCh() chan<- *PrepareMessageEntry {
	return pm.sendPrepareMessageCh
}

// GetTimerCtx returns the context of the timer
func (pm *PhaseManager) GetTimerCtx() context.Context {
	return pm.timerCtx
}

// CancelTimerCtx cancels the context of the timer
func (pm *PhaseManager) CancelTimerCtx() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.timerCancel()
}

// ResetTimerCtx resets the timer context
func (pm *PhaseManager) ResetTimerCtx() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.timerCtx, pm.timerCancel = context.WithCancel(context.Background())
}

// GetProposerCtx returns the context of the proposer
func (pm *PhaseManager) GetProposerCtx() context.Context {
	return pm.proposerCtx
}

// CancelProposerCtx cancels the context of the proposer
func (pm *PhaseManager) CancelProposerCtx() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.proposerCancel()
}

// ResetProposerCtx resets the context of the proposer
func (pm *PhaseManager) ResetProposerCtx() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.proposerCtx, pm.proposerCancel = context.WithCancel(context.Background())
}

// AddPrepareMessageToLog adds a prepare message to the log
func (pm *PhaseManager) AddPrepareMessageToLog(prepareMessageEntry *PrepareMessageEntry) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.prepareMessageLog = append(pm.prepareMessageLog, prepareMessageEntry)
}

// GetPrepareMessageLog returns the prepare message log
func (pm *PhaseManager) GetPrepareMessageLog() []*PrepareMessageEntry {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	return pm.prepareMessageLog
}

// GetHighestValidPrepareMessageInLog gets the highest valid prepare message in the log
func (pm *PhaseManager) GetHighestValidPrepareMessageInLog(expiryTime time.Time) (*pb.BallotNumber, bool) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	highestBallotNumber := pm.state.GetBallotNumber()
	valid := false
	for _, prepareMessageEntry := range pm.prepareMessageLog {
		// Check if ballot number is higher than the highest ballot number in the log
		if compareBallotNumbers(prepareMessageEntry.PrepareMessage.B, highestBallotNumber) == 1 {
			highestBallotNumber = prepareMessageEntry.PrepareMessage.B
			valid = true
			// Check if prepare message is expired
			if expiryTime.Sub(prepareMessageEntry.Timestamp) > prepareTimeout {
				valid = false
			}
		}
	}
	return highestBallotNumber, valid
}

// Reset resets the phase manager
func (pm *PhaseManager) Reset() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.prepareMessageLog = make([]*PrepareMessageEntry, 0)
	// // Drain channel
	// for len(pm.sendPrepareMessageCh) > 0 {
	// 	<-pm.sendPrepareMessageCh
	// }
	// pm.sendPrepareMessageCh = make(chan *PrepareMessageEntry, 10)
	pm.ResetTimerCtx()
	pm.ResetProposerCtx()
	// pm.CancelProposerCtx()
}

// CreatePhaseManager creates a new phase manager instance
func CreatePhaseManager(id string, state *ServerState, timer *SafeTimer) *PhaseManager {
	// The phase manager is created in expired state
	// and the phase change complete channel is used to signal that the phase change is complete
	pm := &PhaseManager{
		mutex:                sync.Mutex{},
		id:                   id,
		prepareMessageLog:    make([]*PrepareMessageEntry, 0),
		sendPrepareMessageCh: make(chan *PrepareMessageEntry, 10),
		state:                state,
		timer:                timer,
	}
	pm.ResetTimerCtx()
	pm.ResetProposerCtx()
	// pm.CancelTimerCtx()
	// pm.CancelProposerCtx()
	return pm
}
