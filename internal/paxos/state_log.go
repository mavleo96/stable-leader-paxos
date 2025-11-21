package paxos

import (
	"fmt"
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	"google.golang.org/protobuf/proto"
)

// StateLog is a struct that contains the state of the paxos system
type StateLog struct {
	id    string
	mutex sync.RWMutex
	log   map[int64]*LogRecord // seq -> record
}

// LogRecord is a struct that contains the log record for a transaction
type LogRecord struct {
	b           *pb.BallotNumber
	sequenceNum int64
	request     *pb.TransactionRequest
	accepted    bool
	committed   bool
	executed    bool
	result      int64
}

// GetSequenceNumber returns the sequence number for a given request
func (s *StateLog) GetSequenceNumber(request *pb.TransactionRequest) int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	for _, record := range s.log {
		if record != nil && proto.Equal(record.request, request) {
			return record.sequenceNum
		}
	}
	return 0
}

// AssignSequenceNumberAndCreateRecord assigns a sequence number to a log record for a given digest and creates a new log record if not found
func (s *StateLog) AssignSequenceNumberAndCreateRecord(ballotNumber *pb.BallotNumber, request *pb.TransactionRequest) (int64, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if request is already in log record
	for sequenceNum := range s.log {
		record, exists := s.log[sequenceNum]
		if !exists {
			continue
		}
		if record != nil && proto.Equal(record.request, request) {
			if BallotNumberIsHigher(record.b, ballotNumber) {
				record.b = ballotNumber
				return record.sequenceNum, true
			}
			return record.sequenceNum, false
		}
	}

	// If request is not in log record, assign new sequence number
	sequenceNum := int64(1)
	if utils.Max(utils.Keys(s.log)) != 0 {
		sequenceNum = utils.Max(utils.Keys(s.log)) + 1
	}
	s.log[sequenceNum] = createLogRecord(ballotNumber, sequenceNum, request)

	return sequenceNum, true
}

// CreateRecordIfNotExists creates a new log record if not found
func (s *StateLog) CreateRecordIfNotExists(ballotNumber *pb.BallotNumber, sequenceNum int64, request *pb.TransactionRequest) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, exists := s.log[sequenceNum]; !exists {
		s.log[sequenceNum] = createLogRecord(ballotNumber, sequenceNum, request)
		return true
	}
	if BallotNumberIsHigher(s.log[sequenceNum].b, ballotNumber) {
		s.log[sequenceNum].b = ballotNumber
		s.log[sequenceNum].request = request
		return true
	}
	return false
}

// Exists returns true if the log record exists for a given sequence number
func (s *StateLog) Exists(sequenceNum int64) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	_, exists := s.log[sequenceNum]
	return exists
}

// Delete deletes the log record for a given sequence number
func (s *StateLog) Delete(sequenceNum int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.log, sequenceNum)
}

// Keys returns the keys of the log
func (s *StateLog) MaxSequenceNum() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if utils.Max(utils.Keys(s.log)) == 0 {
		return 0
	}
	return utils.Max(utils.Keys(s.log))
}

// GetBallotNumber returns the ballot number for a given sequence number
func (s *StateLog) GetBallotNumber(sequenceNum int64) *pb.BallotNumber {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return nil
	}
	return record.b
}

// GetRequest returns the request for a given sequence number
func (s *StateLog) GetRequest(sequenceNum int64) *pb.TransactionRequest {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return nil
	}
	return record.request
}

// SetRequest sets the request for a given sequence number
func (s *StateLog) SetRequest(sequenceNum int64, request *pb.TransactionRequest) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return
	}
	record.request = request
}

// IsAccepted returns true if the transaction is accepted
func (s *StateLog) IsAccepted(sequenceNum int64) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return false
	}
	return record.accepted
}

// IsCommitted returns true if the transaction is committed
func (s *StateLog) IsCommitted(sequenceNum int64) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return false
	}
	return record.committed
}

// IsExecuted returns true if the transaction is executed
func (s *StateLog) IsExecuted(sequenceNum int64) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return false
	}
	return record.executed
}

// SetAccepted sets the accepted flag for a transaction
func (s *StateLog) SetAccepted(sequenceNum int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return
	}
	record.accepted = true
}

// SetCommitted sets the committed flag for a transaction
func (s *StateLog) SetCommitted(sequenceNum int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return
	}
	record.committed = true
	record.accepted = true
}

// SetExecuted sets the executed flag for a transaction
func (s *StateLog) SetExecuted(sequenceNum int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return
	}
	record.executed = true
}

// GetResult returns the result for a transaction
func (s *StateLog) GetResult(sequenceNum int64) int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return 0
	}
	return record.result
}

// SetResult sets the result for a transaction
func (s *StateLog) SetResult(sequenceNum int64, result int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return
	}
	record.result = result
}

// GetAcceptedLog returns the accepted log
func (s *StateLog) GetAcceptedLog() []*pb.AcceptedMessage {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	acceptedLog := make([]*pb.AcceptedMessage, 0)
	for _, record := range s.log {
		acceptedLog = append(acceptedLog, &pb.AcceptedMessage{
			B:           record.b,
			SequenceNum: record.sequenceNum,
			Message:     record.request,
			NodeID:      s.id,
		})
	}
	return acceptedLog
}

// GetLogString returns the log string for a given sequence number
func (s *StateLog) GetLogString(sequenceNum int64) string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	record, exists := s.log[sequenceNum]
	if !exists {
		return fmt.Sprintf("s: %d, status: X", sequenceNum)
	}
	status := statusString(record)
	return fmt.Sprintf("s: %d, status: %s, ballot number: %s, result: %d, request: %s", sequenceNum, status, utils.BallotNumberString(record.b), record.result, utils.TransactionRequestString(record.request))
}

// Reset resets the state log
func (s *StateLog) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.log = make(map[int64]*LogRecord, 100)
}

// statusString returns the status string for a given log record
func statusString(record *LogRecord) string {
	if record == nil {
		return "X"
	}
	if record.executed {
		return "E"
	}
	if record.committed {
		return "C"
	}
	if record.accepted {
		return "A"
	}
	return "X"
}

// CreateStateLog creates a new state log
func CreateStateLog(id string) *StateLog {
	return &StateLog{
		id:    id,
		mutex: sync.RWMutex{},
		log:   make(map[int64]*LogRecord, 100),
	}
}

// CreateLogRecord creates a new log record
func createLogRecord(ballotNumber *pb.BallotNumber, sequenceNumber int64, request *pb.TransactionRequest) *LogRecord {
	return &LogRecord{
		b:           ballotNumber,
		sequenceNum: sequenceNumber,
		request:     request,
		accepted:    false,
		committed:   false,
		executed:    false,
		result:      -1,
	}
}

// ---------------------------------------------------------- //

// LastReply represents a map of sender to last sent reply with a mutex
type LastReply struct {
	mutex    sync.RWMutex
	replyMap map[string]*pb.TransactionResponse
}

// Get returns the last reply sent to a sender
func (l *LastReply) Get(sender string) *pb.TransactionResponse {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.replyMap[sender]
}

// Update updates the last reply sent to a sender
func (l *LastReply) Update(sender string, reply *pb.TransactionResponse) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.replyMap[sender] = reply
}

// Reset resets the last reply
func (l *LastReply) Reset() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.replyMap = make(map[string]*pb.TransactionResponse)
}

// CreateLastReply creates a new last reply
func CreateLastReply() *LastReply {
	return &LastReply{
		mutex:    sync.RWMutex{},
		replyMap: make(map[string]*pb.TransactionResponse, 10),
	}
}
