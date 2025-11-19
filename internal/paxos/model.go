package paxos

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/database"
	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// TODO: Need to decompose this into Proposer and Acceptor structures
type PaxosServer struct {
	SysInitializedMutex sync.Mutex
	SysInitialized      bool
	Mutex               sync.RWMutex
	config              *ServerConfig
	NodeID              string // Compose from Node struct
	Addr                string // Compose from Node struct
	// State               AcceptorState
	state             *ServerState
	DB                *database.Database
	Peers             map[string]*models.Node
	PaxosTimer        *SafeTimer // Proposer only
	AcceptedMessages  []*pb.AcceptedMessage
	PrepareMessageLog map[time.Time]*PrepareRequestRecord // timestamp mapped to prepare message

	// Component Managers
	logger *Logger

	wg sync.WaitGroup

	pb.UnimplementedPaxosNodeServer
}

type PrepareRequestRecord struct {
	ResponseChannel chan *pb.PrepareMessage
	PrepareMessage  *pb.PrepareMessage
}

var UnsuccessfulTransactionResponse = &pb.TransactionResponse{}

var NoOperation = &pb.TransactionRequest{}

func (s *PaxosServer) InitializeSystem() {
	s.SysInitializedMutex.Lock()
	defer s.SysInitializedMutex.Unlock()
	if !s.SysInitialized {
		log.Infof("Initializing system by leader election")
		s.PrepareRoutine()
		s.SysInitialized = true
		log.Infof("System initialized")
		return
	}
	log.Infof("System already initialized")
}

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
	// s.State.Mutex.Lock()
	// s.State.Leader = &models.Node{ID: ""}
	// log.Infof("Setting leader to nil")

	// Decide if there is a latest prepare message
	latestPrepareMessage := FindHighestValidPrepareMessage(s.PrepareMessageLog, time.Now())
	highestBallotNumber := FindHighestBallotNumberInPrepareMessageLog(s.PrepareMessageLog)

	// If there is, set the leader and promised ballot number
	if latestPrepareMessage != nil {
		s.state.SetBallotNumber(latestPrepareMessage.B)
		log.Infof("Promised ballot number set to %s", utils.BallotNumberString(s.state.GetBallotNumber()))
		log.Infof("Leader set to %s", s.state.GetLeader())

		for timestamp, prepareMessageEntry := range s.PrepareMessageLog {
			if prepareMessageEntry.ResponseChannel != nil {
				prepareMessageEntry.ResponseChannel <- latestPrepareMessage
				close(prepareMessageEntry.ResponseChannel)
			}
			delete(s.PrepareMessageLog, timestamp)
		}
		return
	}

	// If there is not, initiate an election, set ballot number and release mutex
	if highestBallotNumber != nil && BallotNumberIsHigher(s.state.GetBallotNumber(), highestBallotNumber) {
		s.state.SetBallotNumber(&pb.BallotNumber{N: highestBallotNumber.N + 1, NodeID: s.NodeID})
	} else {
		s.state.SetBallotNumber(&pb.BallotNumber{N: s.state.GetBallotNumber().N + 1, NodeID: s.NodeID})
	}
	log.Infof("Changed ballot number set to %s", utils.BallotNumberString(s.state.GetBallotNumber()))
	newPrepareMessage := &pb.PrepareMessage{
		B: &pb.BallotNumber{
			N:      s.state.GetBallotNumber().N,
			NodeID: s.NodeID,
		},
	}
	s.logger.AddSentPrepareMessage(newPrepareMessage)
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
	// selfAddress := s.Addr
	// if selfPeer, ok := s.Peers[s.NodeID]; ok && selfPeer != nil && selfPeer.Address != "" {
	// 	selfAddress = selfPeer.Address
	// }
	// s.State.Leader = &models.Node{
	// 	ID:      s.NodeID,
	// 	Address: selfAddress,
	// }
	// log.Infof("Leader for %s is %s", utils.BallotNumberString(newPrepareMessage.B), s.State.Leader.ID)
	log.Infof("Accept log map: %v", acceptLogMap)

	// Find the highest sequence number in the accept log map
	newAcceptLog := make(map[int64]*pb.AcceptRecord)
	for _, acceptLog := range acceptLogMap {
		for _, record := range acceptLog {
			sequenceNum := record.AcceptedSequenceNum
			_, exists := newAcceptLog[sequenceNum]
			if !exists {
				newAcceptLog[sequenceNum] = record
			} else if BallotNumberIsHigher(newAcceptLog[sequenceNum].AcceptedBallotNum, record.AcceptedBallotNum) {
				newAcceptLog[sequenceNum] = record
			}
		}
	}

	maxSequenceNum := s.state.StateLog.MaxSequenceNum()
	for sequenceNum := int64(1); sequenceNum <= maxSequenceNum; sequenceNum++ {
		record, exists := newAcceptLog[sequenceNum]
		if exists {
			s.state.StateLog.CreateRecordIfNotExists(record.AcceptedBallotNum, sequenceNum, record.AcceptedVal)
		} else {
			s.state.StateLog.CreateRecordIfNotExists(s.state.GetBallotNumber(), sequenceNum, NoOperation)
		}
	}

	newAcceptLog2 := make([]*pb.AcceptRecord, 0)
	for sequenceNum := int64(1); sequenceNum <= maxSequenceNum; sequenceNum++ {
		newAcceptLog2 = append(newAcceptLog2, &pb.AcceptRecord{
			AcceptedBallotNum:   s.state.StateLog.GetBallotNumber(sequenceNum),
			AcceptedSequenceNum: sequenceNum,
			AcceptedVal:         s.state.StateLog.GetRequest(sequenceNum),
			Committed:           s.state.StateLog.IsCommitted(sequenceNum),
			Executed:            s.state.StateLog.IsExecuted(sequenceNum),
			Result:              utils.Int64ToBool(s.state.StateLog.GetResult(sequenceNum)),
		})
	}
	newViewMessage := &pb.NewViewMessage{
		B:         s.state.GetBallotNumber(),
		AcceptLog: newAcceptLog2,
	}
	s.state.AddNewViewMessage(newViewMessage)
	// Mutex is released here so that new client requests can be processed
	log.Infof("New view accept log: %v", newViewMessage)
	go s.NewViewRoutine(newViewMessage)
}

func (s *PaxosServer) NewViewRoutine(newViewMessage *pb.NewViewMessage) {
	acceptCountMap, err := s.SendNewViewRequest(newViewMessage)
	if err != nil {
		log.Fatal(err)
	}

	for sequenceNum, acceptCount := range acceptCountMap {
		if acceptCount >= s.config.F+1 {
			log.Infof("Sequence number %d accepted by quorum in new view request", sequenceNum)
		}
		s.state.StateLog.SetCommitted(sequenceNum)
		err := s.SendCommitRequest(&pb.CommitMessage{
			B:           newViewMessage.B,
			SequenceNum: sequenceNum,
			Transaction: newViewMessage.AcceptLog[sequenceNum-1].AcceptedVal,
		})
		s.logger.AddSentCommitMessage(&pb.CommitMessage{
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
	}
}

func (s *PaxosServer) CatchupRoutine() {
	maxSequenceNum := s.state.StateLog.MaxSequenceNum()
	catchupMessage, err := s.SendCatchUpRequest(maxSequenceNum)
	if err != nil {
		log.Warn(err)
		return
	}
	// s.State.Leader = &models.Node{
	// 	ID:      catchupMessage.B.NodeID,
	// 	Address: s.Peers[catchupMessage.B.NodeID].Address,
	// }
	// log.Infof("Leader set to %s", s.State.Leader.ID)
	// log.Infof("Promised ballot number set to %s", utils.BallotNumberString(catchupMessage.B))
	// s.State.PromisedBallotNum = catchupMessage.B
	s.state.SetBallotNumber(catchupMessage.B)
	for _, record := range catchupMessage.AcceptLog {

		s.state.StateLog.CreateRecordIfNotExists(record.AcceptedBallotNum, record.AcceptedSequenceNum, record.AcceptedVal)
		s.state.StateLog.SetCommitted(record.AcceptedSequenceNum)

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

func (s *PaxosServer) Start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.ServerTimeoutRoutine()
	}()

	s.wg.Wait()
}

func CreatePaxosServer(selfNode *models.Node, peerNodes map[string]*models.Node, clients []string, bankDB *database.Database) *PaxosServer {

	serverConfig := CreateServerConfig(int64(len(peerNodes) + 1))
	serverState := CreateServerState()

	i, err := strconv.Atoi(selfNode.ID[1:])
	if err != nil {
		log.Fatal(err)
	}
	paxosTimer := CreateSafeTimer(i, len(peerNodes)+1)

	return &PaxosServer{
		SysInitializedMutex: sync.Mutex{},
		SysInitialized:      false,
		Mutex:               sync.RWMutex{},
		config:              serverConfig,
		NodeID:              selfNode.ID,
		Addr:                selfNode.Address,
		state:               serverState,
		DB:                  bankDB,
		Peers:               peerNodes,
		PaxosTimer:          paxosTimer,
		AcceptedMessages:    make([]*pb.AcceptedMessage, 0),
		PrepareMessageLog:   make(map[time.Time]*PrepareRequestRecord),
		logger:              CreateLogger(),
		wg:                  sync.WaitGroup{},
	}
}
