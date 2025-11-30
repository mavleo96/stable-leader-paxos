package paxos

import (
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	"google.golang.org/protobuf/proto"
)

// ServerState is a struct that contains the state of the paxos server
type ServerState struct {
	mutex                       sync.RWMutex
	id                          string
	b                           *pb.BallotNumber
	leader                      string
	lastExecutedSequenceNum     int64
	lastCheckpointedSequenceNum int64
	forwardedRequestsLog        []*pb.TransactionRequest

	// Self-managed components
	StateLog   *StateLog
	DedupTable *DedupTable
}

// SetLeader sets the leader of the paxos server
func (s *ServerState) SetLeader(leader string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.leader = leader
}

// ResetLeader resets the leader of the paxos server
func (s *ServerState) ResetLeader() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.leader = ""
}

// IsLeader checks if the server is the leader
func (s *ServerState) IsLeader() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.leader == s.id
}

// GetBallotNumber returns the current ballot number
func (s *ServerState) GetBallotNumber() *pb.BallotNumber {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.b
}

// SetBallotNumber sets the current ballot number
func (s *ServerState) SetBallotNumber(b *pb.BallotNumber) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.b = b
}

// TODO: improve design later
// AssignSequenceNumberAndCreateRecord assigns a sequence number to a log record for a given digest and creates a new log record if not found
func (s *ServerState) AssignSequenceNumberAndCreateRecord(ballotNumber *pb.BallotNumber, request *pb.TransactionRequest) (int64, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.StateLog.mutex.Lock()
	defer s.StateLog.mutex.Unlock()

	// Check if request is already in log record
	for sequenceNum := range s.StateLog.log {
		record, exists := s.StateLog.log[sequenceNum]
		if !exists {
			continue
		}
		if record != nil && proto.Equal(record.request, request) {
			if compareBallotNumbers(ballotNumber, record.b) == 1 {
				record.b = ballotNumber
				return record.sequenceNum, true
			}
			return record.sequenceNum, false
		}
	}

	// If request is not in log record, assign new sequence number
	sequenceNum := s.lastCheckpointedSequenceNum + 1
	if utils.Max(utils.Keys(s.StateLog.log)) != 0 {
		sequenceNum = utils.Max(utils.Keys(s.StateLog.log)) + 1
	}
	s.StateLog.log[sequenceNum] = createLogRecord(ballotNumber, sequenceNum, request)

	return sequenceNum, true
}

// GetLastExecutedSequenceNum returns the last executed sequence number
func (s *ServerState) GetLastExecutedSequenceNum() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.lastExecutedSequenceNum
}

// SetLastExecutedSequenceNum sets the last executed sequence number
func (s *ServerState) SetLastExecutedSequenceNum(sequenceNum int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.lastExecutedSequenceNum = sequenceNum
}

// GetLastCheckpointedSequenceNum returns the last checkpointed sequence number
func (s *ServerState) GetLastCheckpointedSequenceNum() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.lastCheckpointedSequenceNum
}

// SetLastCheckpointedSequenceNum sets the last checkpointed sequence number
func (s *ServerState) SetLastCheckpointedSequenceNum(sequenceNum int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.lastCheckpointedSequenceNum = sequenceNum
}

// TODO: improve design later
// MaxSequenceNum returns the maximum sequence number
func (s *ServerState) MaxSequenceNum() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.StateLog.mutex.RLock()
	defer s.StateLog.mutex.RUnlock()
	if utils.Max(utils.Keys(s.StateLog.log)) == 0 {
		return s.lastCheckpointedSequenceNum
	}
	return utils.Max(utils.Keys(s.StateLog.log))
}

// AddForwardedRequest adds a forwarded request to the forwarded requests log
func (s *ServerState) AddForwardedRequest(request *pb.TransactionRequest) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.forwardedRequestsLog = append(s.forwardedRequestsLog, request)
}

// InForwardedRequestsLog checks if a request is in the forwarded requests log
func (s *ServerState) InForwardedRequestsLog(request *pb.TransactionRequest) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	for _, forwardRequest := range s.forwardedRequestsLog {
		if proto.Equal(forwardRequest, request) {
			return true
		}
	}
	return false
}

// ResetForwardedRequestsLog resets the forwarded requests log
func (s *ServerState) ResetForwardedRequestsLog() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.forwardedRequestsLog = make([]*pb.TransactionRequest, 0)
}

// Reset resets the server state
func (s *ServerState) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.b = &pb.BallotNumber{N: 1, NodeID: "n1"}
	s.leader = "n1"
	s.lastExecutedSequenceNum = 0
	s.lastCheckpointedSequenceNum = 0
	s.forwardedRequestsLog = make([]*pb.TransactionRequest, 0)
	s.StateLog.Reset()
	s.DedupTable.Reset()
}

// CreateServerState creates a new server state
func CreateServerState(id string) *ServerState {
	return &ServerState{
		mutex:                       sync.RWMutex{},
		id:                          id,
		b:                           &pb.BallotNumber{N: 1, NodeID: "n1"},
		leader:                      "n1",
		lastExecutedSequenceNum:     0,
		lastCheckpointedSequenceNum: 0,
		forwardedRequestsLog:        make([]*pb.TransactionRequest, 0),
		StateLog:                    CreateStateLog(id),
		DedupTable:                  CreateDedupTable(),
	}
}
