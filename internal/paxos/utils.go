package paxos

import (
	"crypto/rand"
	"math/big"
	"time"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

// Response Utilities

// UnsuccessfulTransactionResponse is the response returned when a transaction is unsuccessful
var UnsuccessfulTransactionResponse = &pb.TransactionResponse{}

// NoOperation is the request returned when no operation is needed
var NoOperation = &pb.TransactionRequest{}

// BallotNumber Utilities

// ballotNumberIsHigherOrEqual checks if current >= new
func ballotNumberIsHigherOrEqual(current *pb.BallotNumber, new *pb.BallotNumber) bool {
	if new.N == current.N {
		return new.NodeID >= current.NodeID
	}
	return new.N >= current.N
}

// ballotNumberIsHigher checks if current > new
func ballotNumberIsHigher(current *pb.BallotNumber, new *pb.BallotNumber) bool {
	if new.N == current.N {
		return new.NodeID > current.NodeID
	}
	return new.N > current.N
}

// Timer Utilities

// RandomTimeout returns a random timeout between min and max
func RandomTimeout(min time.Duration, max time.Duration) (time.Duration, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(max.Milliseconds()-min.Milliseconds()))
	return time.Duration(n.Int64()+min.Milliseconds()) * time.Millisecond, err
}

// Message Utilities

// aggregateAckMessages aggregates the accepted messages from all ack messages
func aggregateAckMessages(newBallotNumber *pb.BallotNumber, ackMessages []*pb.AckMessage) (int64, []*pb.AcceptMessage) {
	// Get highest checkpointed sequence number in ack messages
	highestCheckpointedSequenceNum := int64(0)
	for _, ackMessage := range ackMessages {
		if ackMessage.SequenceNum > highestCheckpointedSequenceNum {
			highestCheckpointedSequenceNum = ackMessage.SequenceNum
		}
	}

	// Aggregate accepted messages from all ack messages, starting from highest checkpointed sequence number
	acceptedMessagesMap := make(map[int64]*pb.AcceptedMessage, 100)
	maxSequenceNum := highestCheckpointedSequenceNum
	for _, ackMessage := range ackMessages {
		for _, acceptedMessage := range ackMessage.AcceptLog {
			sequenceNum := acceptedMessage.SequenceNum
			if sequenceNum <= highestCheckpointedSequenceNum {
				continue
			}
			if sequenceNum > maxSequenceNum {
				maxSequenceNum = sequenceNum
			}
			if msg, exists := acceptedMessagesMap[sequenceNum]; !exists || ballotNumberIsHigher(msg.B, acceptedMessage.B) {
				acceptedMessagesMap[sequenceNum] = acceptedMessage
			}
		}
	}

	// Created sorted accept messages; fill in the gaps with no-op messages
	sortedAcceptMessages := make([]*pb.AcceptMessage, maxSequenceNum-highestCheckpointedSequenceNum)
	for sequenceNum := highestCheckpointedSequenceNum + 1; sequenceNum <= maxSequenceNum; sequenceNum++ {
		idx := sequenceNum - highestCheckpointedSequenceNum - 1
		if msg, exists := acceptedMessagesMap[sequenceNum]; exists {
			sortedAcceptMessages[idx] = &pb.AcceptMessage{
				B:           newBallotNumber,
				SequenceNum: sequenceNum,
				Message:     msg.Message,
			}
		} else {
			sortedAcceptMessages[idx] = &pb.AcceptMessage{
				B:           newBallotNumber,
				SequenceNum: sequenceNum,
				Message:     NoOperation,
			}
		}
	}
	return highestCheckpointedSequenceNum, sortedAcceptMessages
}
