package paxos

import (
	"sync"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
	"google.golang.org/protobuf/proto"
)

// ServerState is a struct that contains the state of the paxos server
type ServerState struct {
	mutex                   sync.RWMutex
	id                      string
	b                       *pb.BallotNumber
	leader                  string
	lastExecutedSequenceNum int64

	forwardedRequestsLog []*pb.TransactionRequest

	// Self-managed components
	StateLog  *StateLog
	LastReply *LastReply
}

// Leader is the leader of the paxos server
func (s *ServerState) GetLeader() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.leader
}

// SetLeader sets the leader of the paxos server
func (s *ServerState) SetLeader(leader string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.leader = leader
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
	s.b = &pb.BallotNumber{N: 0, NodeID: ""}
	s.lastExecutedSequenceNum = 0
	s.forwardedRequestsLog = make([]*pb.TransactionRequest, 0)
	s.StateLog.Reset()
	s.LastReply.Reset()
}

// CreateServerState creates a new server state
func CreateServerState(id string) *ServerState {
	return &ServerState{
		mutex:                   sync.RWMutex{},
		id:                      id,
		b:                       &pb.BallotNumber{N: 1, NodeID: "n1"},
		leader:                  "n1",
		lastExecutedSequenceNum: 0,
		forwardedRequestsLog:    make([]*pb.TransactionRequest, 0),
		StateLog:                CreateStateLog(id),
		LastReply:               CreateLastReply(),
	}
}
