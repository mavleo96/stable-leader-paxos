package paxos

import (
	"sync"
	"time"

	"github.com/mavleo96/cft-mavleo96/internal/database"
	"github.com/mavleo96/cft-mavleo96/internal/models"
	"github.com/mavleo96/cft-mavleo96/internal/utils"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
)

const (
	minBackupTimeout = time.Duration(300 * time.Millisecond)
	maxBackupTimeout = time.Duration(400 * time.Millisecond)
	prepareTimeout   = time.Duration(150 * time.Millisecond)
)

// TODO: Need to decompose this into Proposer and Acceptor structures
type PaxosServer struct {
	Mutex             sync.RWMutex
	IsAlive           bool
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

var NoOperation = &pb.TransactionRequest{}

func (s *PaxosServer) ServerTimeoutRoutine() {
	log.Warn("Server timeout routine is active")

	for timeoutCount := 1; ; timeoutCount++ {
		<-s.PaxosTimer.TimeoutChannel
		log.Warnf("Backup timer has expired: %d at %d", timeoutCount, time.Now().UnixMilli())
		go s.PrepareRoutine()
	}
}

// PrepareRoutine is used to prepare the leader for an election
func (s *PaxosServer) PrepareRoutine() {
	log.Info("Prepare routine has been called")
	// Reset Leader
	s.State.Mutex.Lock()
	s.State.Leader = &models.Node{ID: ""}
	log.Infof("Setting leader to nil")

	// Decide if there is a latest prepare message
	latestPrepareMessage := FindHighestValidPrepareMessage(s.PrepareMessageLog, time.Now())
	highestBallotNumber := FindHighestBallotNumberInPrepareMessageLog(s.PrepareMessageLog)

	// If there is, set the leader and promised ballot number
	if latestPrepareMessage != nil {
		s.State.Leader = &models.Node{
			ID:      latestPrepareMessage.B.NodeID,
			Address: s.Peers[latestPrepareMessage.B.NodeID].Address,
		}
		s.State.PromisedBallotNum = latestPrepareMessage.B
		log.Infof("Promised ballot number set to %s", utils.BallotNumberString(s.State.PromisedBallotNum))
		log.Infof("Leader set to %s", s.State.Leader.ID)

		for timestamp, prepareMessageEntry := range s.PrepareMessageLog {
			prepareMessageEntry.ResponseChannel <- latestPrepareMessage
			close(prepareMessageEntry.ResponseChannel)
			delete(s.PrepareMessageLog, timestamp)
		}
		s.State.Mutex.Unlock()
		return
	}

	// If there is not, initiate an election, set ballot number and release mutex
	if highestBallotNumber != nil && BallotNumberIsHigher(s.State.PromisedBallotNum, highestBallotNumber) {
		s.State.PromisedBallotNum = &pb.BallotNumber{N: highestBallotNumber.N + 1, NodeID: s.NodeID}
	} else {
		s.State.PromisedBallotNum = &pb.BallotNumber{N: s.State.PromisedBallotNum.N + 1, NodeID: s.NodeID}
	}
	log.Infof("Changed ballot number set to %s", utils.BallotNumberString(s.State.PromisedBallotNum))
	newPrepareMessage := &pb.PrepareMessage{
		B: &pb.BallotNumber{
			N:      s.State.PromisedBallotNum.N,
			NodeID: s.NodeID,
		},
	}
	s.State.Mutex.Unlock()
	ok, acceptLogMap, err := s.SendPrepareRequest(newPrepareMessage)
	if err != nil {
		log.Warn(err)
		return
	}
	if !ok {
		log.Warnf("Failed to get a quorum of promises for %s", utils.BallotNumberString(newPrepareMessage.B))
		return
	}

	// If election won
	// acquire mutex -> set leader -> synchronize accept log -> send new view request -> release mutex
	s.State.Mutex.Lock()
	s.State.Leader = &models.Node{
		ID:      s.NodeID,
		Address: s.Peers[s.NodeID].Address,
	}
	log.Infof("Leader for %s is %s", utils.BallotNumberString(newPrepareMessage.B), s.State.Leader.ID)
	log.Infof("Accept log map: %v", acceptLogMap)

	// Find the highest sequence number in the accept log map
	for _, acceptLog := range acceptLogMap {
		for _, record := range acceptLog {
			sequenceNum := record.AcceptedSequenceNum
			currentRecord, ok := s.State.AcceptLog[sequenceNum]
			if ok {
				if BallotNumberIsHigher(currentRecord.AcceptedBallotNum, record.AcceptedBallotNum) {
					s.State.AcceptLog[sequenceNum].AcceptedBallotNum = record.AcceptedBallotNum
					s.State.AcceptLog[sequenceNum].AcceptedVal = record.AcceptedVal
				}
			} else {
				s.State.AcceptLog[sequenceNum] = &pb.AcceptRecord{
					AcceptedBallotNum:   record.AcceptedBallotNum,
					AcceptedSequenceNum: record.AcceptedSequenceNum,
					AcceptedVal:         record.AcceptedVal,
					Committed:           false,
					Executed:            false,
					Result:              false,
				}
			}
		}
	}
	maxSequenceNum := MaxSequenceNumber(s.State.AcceptLog)
	for sequenceNum := int64(1); sequenceNum <= maxSequenceNum; sequenceNum++ {
		_, ok := s.State.AcceptLog[sequenceNum]
		if ok {
			s.State.AcceptLog[sequenceNum].AcceptedBallotNum = s.State.PromisedBallotNum
		} else {
			s.State.AcceptLog[sequenceNum] = &pb.AcceptRecord{
				AcceptedBallotNum:   s.State.PromisedBallotNum,
				AcceptedSequenceNum: sequenceNum,
				AcceptedVal:         NoOperation,
				Committed:           false,
				Executed:            false,
				Result:              false,
			}
		}
	}
	newAcceptLog := make([]*pb.AcceptRecord, 0)
	for sequenceNum := int64(1); sequenceNum <= maxSequenceNum; sequenceNum++ {
		newAcceptLog = append(newAcceptLog, s.State.AcceptLog[sequenceNum])
	}
	newViewMessage := &pb.NewViewMessage{
		B:         s.State.PromisedBallotNum,
		AcceptLog: newAcceptLog,
	}
	// Mutex is released here so that new client requests can be processed
	s.State.Mutex.Unlock()
	log.Infof("New view accept log: %v", newViewMessage)
	go s.NewViewRoutine(newViewMessage)
}

func (s *PaxosServer) NewViewRoutine(newViewMessage *pb.NewViewMessage) {
	acceptCountMap, err := s.SendNewViewRequest(newViewMessage)
	if err != nil {
		log.Fatal(err)
	}

	for sequenceNum, acceptCount := range acceptCountMap {
		if acceptCount >= s.Quorum {
			log.Infof("Sequence number %d accepted by quorum in new view request", sequenceNum)
		}
		s.State.Mutex.Lock()
		s.State.AcceptLog[sequenceNum].Committed = true
		err := s.SendCommitRequest(&pb.CommitMessage{
			B:           newViewMessage.B,
			SequenceNum: sequenceNum,
			Transaction: newViewMessage.AcceptLog[sequenceNum-1].AcceptedVal,
		})
		if err != nil {
			log.Fatal(err)
		}
		_, err = s.TryExecute(sequenceNum)
		if err != nil {
			log.Infof("Failed to execute request %s", utils.TransactionRequestString(newViewMessage.AcceptLog[sequenceNum-1].AcceptedVal))
		}
		s.State.Mutex.Unlock()
	}
}

func (s *PaxosServer) CatchupRoutine() {
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()

	maxSequenceNum := MaxSequenceNumber(s.State.AcceptLog)
	catchupMessage, err := s.SendCatchUpRequest(maxSequenceNum)
	if err != nil {
		log.Warn(err)
		return
	}
	s.State.Leader = &models.Node{
		ID:      catchupMessage.B.NodeID,
		Address: s.Peers[catchupMessage.B.NodeID].Address,
	}
	log.Infof("Leader set to %s", s.State.Leader.ID)
	log.Infof("Promised ballot number set to %s", utils.BallotNumberString(catchupMessage.B))
	s.State.PromisedBallotNum = catchupMessage.B
	for _, record := range catchupMessage.AcceptLog {
		s.State.AcceptLog[record.AcceptedSequenceNum] = &pb.AcceptRecord{
			AcceptedBallotNum:   record.AcceptedBallotNum,
			AcceptedSequenceNum: record.AcceptedSequenceNum,
			AcceptedVal:         record.AcceptedVal,
			Committed:           true,
			Executed:            false,
		}
		_, err = s.TryExecute(record.AcceptedSequenceNum)
		if err != nil {
			log.Infof("Failed to execute request %s", utils.TransactionRequestString(record.AcceptedVal))
		}
	}
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

func FindHighestBallotNumberInPrepareMessageLog(messageLog map[time.Time]*PrepareRequestRecord) *pb.BallotNumber {
	var highestBallotNumber *pb.BallotNumber
	for _, prepareMessageEntry := range messageLog {
		if highestBallotNumber == nil {
			highestBallotNumber = prepareMessageEntry.PrepareMessage.B
		} else if BallotNumberIsHigher(highestBallotNumber, prepareMessageEntry.PrepareMessage.B) {
			highestBallotNumber = prepareMessageEntry.PrepareMessage.B
		}
	}
	return highestBallotNumber
}
