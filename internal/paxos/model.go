package paxos

import (
	"sync"
	"time"

	"github.com/mavleo96/cft-mavleo96/internal/database"
	"github.com/mavleo96/cft-mavleo96/internal/models"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
)

const (
	mainTimeout = 10 * time.Second
	// prepareTimeout = 5 * time.Second
)

// TODO: Need to decompose this into Proposer and Acceptor structures
type PaxosServer struct {
	Mutex              sync.RWMutex
	NodeID             string // Compose from Node struct
	Addr               string // Compose from Node struct
	State              AcceptorState
	DB                 *database.Database
	LastReplyTimestamp map[string]int64 // Proposer only
	// Leader             string           // Proposer only
	Peers  []*models.Node // Compose from Node struct or Proposer
	Quorum int            // Compose from Proposer
	// CurrentBallotNum    *pb.BallotNumber
	CurrentSequenceNum int64            // Proposer only
	CurrentBallotNum   *pb.BallotNumber // Proposer only
	PaxosTimer         *time.Timer      // Proposer only
	// ExecutedSequenceNum int64
	// PendingExecutions []int64
	AcceptedMessages []*pb.AcceptedMessage
	// ProposerClient ProposerClient
	pb.UnimplementedPaxosServer
	// bankpb.UnimplementedTransactionServiceServer
	// bank.BankServer
}

type AcceptorState struct {
	Mutex               sync.RWMutex
	Leader              string
	PromisedBallotNum   *pb.BallotNumber
	AcceptLog           map[int64]*pb.AcceptRecord
	ExecutedSequenceNum int64
	// AcceptLog         []*pb.AcceptRecord // TODO: should this be a map?
}

var UnsuccessfulTransactionResponse = &pb.TransactionResponse{}
