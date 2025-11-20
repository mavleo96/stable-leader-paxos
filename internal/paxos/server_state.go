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
	preparePhase            bool
	lastExecutedSequenceNum int64

	newViewLog           []*pb.NewViewMessage
	forwardedRequestsLog []*pb.TransactionRequest

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

// InPreparePhase checks if the server is in the prepare phase
func (s *ServerState) InPreparePhase() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.preparePhase
}

// SetPreparePhase sets the prepare phase
func (s *ServerState) SetPreparePhase(preparePhase bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.preparePhase = preparePhase
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
	s.newViewLog = make([]*pb.NewViewMessage, 0)
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
		lastExecutedSequenceNum: 0,
		newViewLog:              make([]*pb.NewViewMessage, 0),
		forwardedRequestsLog:    make([]*pb.TransactionRequest, 0),
		StateLog:                CreateStateLog(id),
		LastReply:               CreateLastReply(),
	}
}
