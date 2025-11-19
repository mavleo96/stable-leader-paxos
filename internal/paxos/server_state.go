package paxos

import (
	"sync"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

// ServerState is a struct that contains the state of the paxos server
type ServerState struct {
	mutex                   sync.RWMutex
	b                       *pb.BallotNumber
	lastExecutedSequenceNum int64

	newViewLog []*pb.NewViewMessage

	// Self-managed components
	StateLog  *StateLog
	LastReply *LastReply
}

// Leader is the leader of the paxos server
func (s *ServerState) GetLeader() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.b.NodeID
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

// AddNewViewMessage adds a new view message to the new view log
func (s *ServerState) AddNewViewMessage(newViewMessage *pb.NewViewMessage) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.newViewLog = append(s.newViewLog, newViewMessage)
}

// GetNewViewMessages returns the new view messages
func (s *ServerState) GetNewViewMessages() []*pb.NewViewMessage {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.newViewLog
}

// Reset resets the server state
func (s *ServerState) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.b = &pb.BallotNumber{N: 0, NodeID: ""}
	s.lastExecutedSequenceNum = 0
	s.newViewLog = make([]*pb.NewViewMessage, 0)
	s.StateLog.Reset()
	s.LastReply.Reset()
}

// CreateServerState creates a new server state
func CreateServerState() *ServerState {
	return &ServerState{
		mutex:                   sync.RWMutex{},
		b:                       &pb.BallotNumber{N: 0, NodeID: ""},
		lastExecutedSequenceNum: 0,
		newViewLog:              make([]*pb.NewViewMessage, 0),
		StateLog:                CreateStateLog(),
		LastReply:               CreateLastReply(),
	}
}
