package paxos

import (
	"sync"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

type Logger struct {
	mutex                        sync.RWMutex
	sentPrepareMessages          []*pb.PrepareMessage
	receivedPrepareMessages      []*pb.PrepareMessage
	sentAckMessages              []*pb.AckMessage
	receivedAckMessages          []*pb.AckMessage
	sentAcceptMessages           []*pb.AcceptMessage
	receivedAcceptMessages       []*pb.AcceptMessage
	sentAcceptedMessages         []*pb.AcceptedMessage
	receivedAcceptedMessages     []*pb.AcceptedMessage
	sentCommitMessages           []*pb.CommitMessage
	receivedCommitMessages       []*pb.CommitMessage
	sentNewViewMessages          []*pb.NewViewMessage
	receivedNewViewMessages      []*pb.NewViewMessage
	sentCheckpointMessages       []*pb.CheckpointMessage
	receivedCheckpointMessages   []*pb.CheckpointMessage
	receivedTransactionRequests  []*pb.TransactionRequest
	forwardedTransactionRequests []*pb.TransactionRequest
	sentTransactionResponses     []*pb.TransactionResponse
}

// AddSentPrepareMessage adds a sent prepare message to the logger
func (l *Logger) AddSentPrepareMessage(message *pb.PrepareMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sentPrepareMessages = append(l.sentPrepareMessages, message)
}

// GetSentPrepareMessages returns the sent prepare messages
func (l *Logger) GetSentPrepareMessages() []*pb.PrepareMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.sentPrepareMessages
}

// AddReceivedPrepareMessage adds a received prepare message to the logger
func (l *Logger) AddReceivedPrepareMessage(message *pb.PrepareMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.receivedPrepareMessages = append(l.receivedPrepareMessages, message)
}

// GetReceivedPrepareMessages returns the received prepare messages
func (l *Logger) GetReceivedPrepareMessages() []*pb.PrepareMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.receivedPrepareMessages
}

// AddSentAckMessage adds a sent ack message to the logger
func (l *Logger) AddSentAckMessage(message *pb.AckMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sentAckMessages = append(l.sentAckMessages, message)
}

// GetSentAckMessages returns the sent ack messages
func (l *Logger) GetSentAckMessages() []*pb.AckMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.sentAckMessages
}

// AddReceivedAckMessage adds a received ack message to the logger
func (l *Logger) AddReceivedAckMessage(message *pb.AckMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.receivedAckMessages = append(l.receivedAckMessages, message)
}

// GetReceivedAckMessages returns the received ack messages
func (l *Logger) GetReceivedAckMessages() []*pb.AckMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.receivedAckMessages
}

// AddSentAcceptMessage adds a sent accept message to the logger
func (l *Logger) AddSentAcceptMessage(message *pb.AcceptMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sentAcceptMessages = append(l.sentAcceptMessages, message)
}

// GetSentAcceptMessages returns the sent accept messages
func (l *Logger) GetSentAcceptMessages() []*pb.AcceptMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.sentAcceptMessages
}

// AddReceivedAcceptMessage adds a received accept message to the logger
func (l *Logger) AddReceivedAcceptMessage(message *pb.AcceptMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.receivedAcceptMessages = append(l.receivedAcceptMessages, message)
}

// GetReceivedAcceptMessages returns the received accept messages
func (l *Logger) GetReceivedAcceptMessages() []*pb.AcceptMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.receivedAcceptMessages
}

// AddSentAcceptedMessage adds a sent accepted message to the logger
func (l *Logger) AddSentAcceptedMessage(message *pb.AcceptedMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sentAcceptedMessages = append(l.sentAcceptedMessages, message)
}

// GetSentAcceptedMessages returns the sent accepted messages
func (l *Logger) GetSentAcceptedMessages() []*pb.AcceptedMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.sentAcceptedMessages
}

// AddReceivedAcceptedMessage adds a received accepted message to the logger
func (l *Logger) AddReceivedAcceptedMessage(message *pb.AcceptedMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.receivedAcceptedMessages = append(l.receivedAcceptedMessages, message)
}

// GetReceivedAcceptedMessages returns the received accepted messages
func (l *Logger) GetReceivedAcceptedMessages() []*pb.AcceptedMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.receivedAcceptedMessages
}

// AddSentCommitMessage adds a sent commit message to the logger
func (l *Logger) AddSentCommitMessage(message *pb.CommitMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sentCommitMessages = append(l.sentCommitMessages, message)
}

// GetSentCommitMessages returns the sent commit messages
func (l *Logger) GetSentCommitMessages() []*pb.CommitMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.sentCommitMessages
}

// AddReceivedCommitMessage adds a received commit message to the logger
func (l *Logger) AddReceivedCommitMessage(message *pb.CommitMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.receivedCommitMessages = append(l.receivedCommitMessages, message)
}

// GetReceivedCommitMessages returns the received commit messages
func (l *Logger) GetReceivedCommitMessages() []*pb.CommitMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.receivedCommitMessages
}

// AddSentNewViewMessage adds a sent new view message to the logger
func (l *Logger) AddSentNewViewMessage(message *pb.NewViewMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sentNewViewMessages = append(l.sentNewViewMessages, message)
}

// GetSentNewViewMessages returns the sent new view messages
func (l *Logger) GetSentNewViewMessages() []*pb.NewViewMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.sentNewViewMessages
}

// AddReceivedNewViewMessage adds a received new view message to the logger
func (l *Logger) AddReceivedNewViewMessage(message *pb.NewViewMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.receivedNewViewMessages = append(l.receivedNewViewMessages, message)
}

// GetReceivedNewViewMessages returns the received new view messages
func (l *Logger) GetReceivedNewViewMessages() []*pb.NewViewMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.receivedNewViewMessages
}

// AddSentCheckpointMessage adds a sent checkpoint message to the logger
func (l *Logger) AddSentCheckpointMessage(message *pb.CheckpointMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sentCheckpointMessages = append(l.sentCheckpointMessages, message)
}

// GetSentCheckpointMessages returns the sent checkpoint messages
func (l *Logger) GetSentCheckpointMessages() []*pb.CheckpointMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.sentCheckpointMessages
}

// AddReceivedCheckpointMessage adds a received checkpoint message to the logger
func (l *Logger) AddReceivedCheckpointMessage(message *pb.CheckpointMessage) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.receivedCheckpointMessages = append(l.receivedCheckpointMessages, message)
}

// GetReceivedCheckpointMessages returns the received checkpoint messages
func (l *Logger) GetReceivedCheckpointMessages() []*pb.CheckpointMessage {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.receivedCheckpointMessages
}

// AddReceivedTransactionRequest adds a received transaction request to the logger
func (l *Logger) AddReceivedTransactionRequest(message *pb.TransactionRequest) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.receivedTransactionRequests = append(l.receivedTransactionRequests, message)
}

// GetReceivedTransactionRequests returns the received transaction requests
func (l *Logger) GetReceivedTransactionRequests() []*pb.TransactionRequest {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.receivedTransactionRequests
}

// AddForwardedTransactionRequest adds a forwarded transaction request to the logger
func (l *Logger) AddForwardedTransactionRequest(message *pb.TransactionRequest) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.forwardedTransactionRequests = append(l.forwardedTransactionRequests, message)
}

// GetForwardedTransactionRequests returns the forwarded transaction requests
func (l *Logger) GetForwardedTransactionRequests() []*pb.TransactionRequest {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.forwardedTransactionRequests
}

// AddSentTransactionResponse adds a sent transaction response to the logger
func (l *Logger) AddSentTransactionResponse(message *pb.TransactionResponse) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sentTransactionResponses = append(l.sentTransactionResponses, message)
}

// GetSentTransactionResponses returns the sent transaction responses
func (l *Logger) GetSentTransactionResponses() []*pb.TransactionResponse {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.sentTransactionResponses
}

// Reset resets the logger
func (l *Logger) Reset() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sentPrepareMessages = make([]*pb.PrepareMessage, 0)
	l.receivedPrepareMessages = make([]*pb.PrepareMessage, 0)
	l.sentAckMessages = make([]*pb.AckMessage, 0)
	l.receivedAckMessages = make([]*pb.AckMessage, 0)
	l.sentAcceptMessages = make([]*pb.AcceptMessage, 0)
	l.receivedAcceptMessages = make([]*pb.AcceptMessage, 0)
	l.sentAcceptedMessages = make([]*pb.AcceptedMessage, 0)
	l.receivedAcceptedMessages = make([]*pb.AcceptedMessage, 0)
	l.sentCommitMessages = make([]*pb.CommitMessage, 0)
	l.receivedCommitMessages = make([]*pb.CommitMessage, 0)
	l.sentNewViewMessages = make([]*pb.NewViewMessage, 0)
	l.receivedNewViewMessages = make([]*pb.NewViewMessage, 0)
	l.sentCheckpointMessages = make([]*pb.CheckpointMessage, 0)
	l.receivedCheckpointMessages = make([]*pb.CheckpointMessage, 0)
	l.receivedTransactionRequests = make([]*pb.TransactionRequest, 0)
	l.forwardedTransactionRequests = make([]*pb.TransactionRequest, 0)
	l.sentTransactionResponses = make([]*pb.TransactionResponse, 0)
}

// CreateLogger creates a new logger
func CreateLogger() *Logger {
	return &Logger{
		mutex:                        sync.RWMutex{},
		sentPrepareMessages:          make([]*pb.PrepareMessage, 0),
		receivedPrepareMessages:      make([]*pb.PrepareMessage, 0),
		sentAckMessages:              make([]*pb.AckMessage, 0),
		receivedAckMessages:          make([]*pb.AckMessage, 0),
		sentAcceptMessages:           make([]*pb.AcceptMessage, 0),
		receivedAcceptMessages:       make([]*pb.AcceptMessage, 0),
		sentAcceptedMessages:         make([]*pb.AcceptedMessage, 0),
		receivedAcceptedMessages:     make([]*pb.AcceptedMessage, 0),
		sentCommitMessages:           make([]*pb.CommitMessage, 0),
		receivedCommitMessages:       make([]*pb.CommitMessage, 0),
		sentNewViewMessages:          make([]*pb.NewViewMessage, 0),
		receivedNewViewMessages:      make([]*pb.NewViewMessage, 0),
		sentCheckpointMessages:       make([]*pb.CheckpointMessage, 0),
		receivedCheckpointMessages:   make([]*pb.CheckpointMessage, 0),
		receivedTransactionRequests:  make([]*pb.TransactionRequest, 0),
		forwardedTransactionRequests: make([]*pb.TransactionRequest, 0),
		sentTransactionResponses:     make([]*pb.TransactionResponse, 0),
	}
}
