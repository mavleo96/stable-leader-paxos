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
	ForceTimerExpired chan bool // Proposer only
	// ProposerClient ProposerClient
	pb.UnimplementedPaxosServer
	// bankpb.UnimplementedTransactionServiceServer
	// bank.BankServer
}

type AcceptorState struct {
	Mutex               sync.RWMutex
	Leader              *models.Node
	PromisedBallotNum   *pb.BallotNumber
	AcceptLog           map[int64]*pb.AcceptRecord
	ExecutedSequenceNum int64
	// AcceptLog         []*pb.AcceptRecord // TODO: should this be a map?
}

var UnsuccessfulTransactionResponse = &pb.TransactionResponse{}

func (s *PaxosServer) TimerRoutine() {
	log.Info("Timer routine started is active")
	for {
		select {
		case <-s.PaxosTimer.Timer.C:
			log.Fatal("Timer expired, need to initiate prepare")
		case <-s.ForceTimerExpired:
			s.PaxosTimer.TimerCleanup()
			log.Fatal("Timer expired, need to initiate prepare")
		}
	}
}
