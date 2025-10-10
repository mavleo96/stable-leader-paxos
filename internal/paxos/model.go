package paxos

import (
	"sync"
	"time"

	"github.com/mavleo96/cft-mavleo96/internal/database"
	"github.com/mavleo96/cft-mavleo96/internal/models"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
)

const (
	minBackupTimeout = time.Duration(50 * time.Millisecond)
	maxBackupTimeout = time.Duration(100 * time.Millisecond)
	prepareTimeout   = time.Duration(75 * time.Millisecond)
)

// TODO: Need to decompose this into Proposer and Acceptor structures
type PaxosServer struct {
	Mutex             sync.RWMutex
	NodeID            string // Compose from Node struct
	Addr              string // Compose from Node struct
	State             AcceptorState
	DB                *database.Database
	LastReply         map[string]*pb.TransactionResponse // Proposer only
	Peers             map[string]*models.Node
	Quorum            int        // Compose from Proposer
	PaxosTimer        *SafeTimer // Proposer only
	AcceptedMessages  []*pb.AcceptedMessage
	PrepareMessageLog map[time.Time]*PrepareRequestRecord // timestamp mapped to prepare message
	pb.UnimplementedPaxosServer
}

type PrepareRequestRecord struct {
	ResponseChannel chan *pb.PrepareMessage
	PrepareMessage  *pb.PrepareMessage
}

type AcceptorState struct {
	Mutex               sync.RWMutex
	Leader              *models.Node
	PromisedBallotNum   *pb.BallotNumber
	AcceptLog           map[int64]*pb.AcceptRecord
	ExecutedSequenceNum int64
}

var UnsuccessfulTransactionResponse = &pb.TransactionResponse{}

func (s *PaxosServer) ServerTimeoutRoutine() {
	log.Warn("Server timeout routine is active")

	for timeoutCount := 1; ; timeoutCount++ {
		<-s.PaxosTimer.TimeoutChannel
		log.Warnf("Backup timer has expired: %d at %d", timeoutCount, time.Now().UnixMilli())
		s.PrepareRoutine()
	}
}

func (s *PaxosServer) PrepareRoutine() {
	// Reset Leader
	s.State.Mutex.Lock()
	s.State.Leader = &models.Node{ID: ""}
	log.Infof("Setting leader to nil")
	// s.State.Mutex.Unlock()

	// Decide if there is a latest prepare message
	latestPrepareMessage := FindHighestValidPrepareMessage(s.PrepareMessageLog, time.Now())

	// If there is, set the leader and promised ballot number
	if latestPrepareMessage != nil {
		// s.State.Mutex.Lock()
		s.State.Leader = &models.Node{
			ID:      latestPrepareMessage.B.NodeID,
			Address: s.Peers[latestPrepareMessage.B.NodeID].Address,
		}
		s.State.PromisedBallotNum = latestPrepareMessage.B
		// s.State.Mutex.Unlock()

		for timestamp, prepareMessageEntry := range s.PrepareMessageLog {
			prepareMessageEntry.ResponseChannel <- latestPrepareMessage
			close(prepareMessageEntry.ResponseChannel)
			delete(s.PrepareMessageLog, timestamp)
		}
		s.State.Mutex.Unlock()
		return
	}

	// If there is not, initiate an election, set ballot number and release mutex
	s.State.PromisedBallotNum = &pb.BallotNumber{N: s.State.PromisedBallotNum.N + 1, NodeID: s.NodeID}
	newPrepareMessage := &pb.PrepareMessage{
		B: &pb.BallotNumber{
			N:      s.State.PromisedBallotNum.N,
			NodeID: s.NodeID,
		},
	}
	s.State.Mutex.Unlock()
	ok, err := s.SendPrepareRequest(newPrepareMessage)
	if err != nil {
		log.Warn(err)
		return
	}
	if !ok {
		log.Warn("Failed to get a quorum of promises")
		return
	}

	// If election won acquire mutex and set leader
	s.State.Mutex.Lock()
	s.State.Leader = &models.Node{
		ID:      newPrepareMessage.B.NodeID,
		Address: s.Peers[newPrepareMessage.B.NodeID].Address,
	}
	s.State.Mutex.Unlock()
}

func FindHighestValidPrepareMessage(messageLog map[time.Time]*PrepareRequestRecord, expiryTimeStamp time.Time) *pb.PrepareMessage {
	var latestPrepareMessage *pb.PrepareMessage
	for timestamp, prepareMessageEntry := range messageLog {
		if expiryTimeStamp.Sub(timestamp) < prepareTimeout {
			if latestPrepareMessage == nil {
				latestPrepareMessage = prepareMessageEntry.PrepareMessage
			} else if BallotNumberIsHigher(latestPrepareMessage.B, prepareMessageEntry.PrepareMessage.B) {
				latestPrepareMessage = prepareMessageEntry.PrepareMessage
			}
		}
	}
	return latestPrepareMessage
}
