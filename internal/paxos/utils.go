package paxos

import (
	"cmp"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

// Response Constants

// EmptyTransactionResponse is the response returned when a transaction is unsuccessful
var EmptyTransactionResponse = &pb.TransactionResponse{}

// NoOperation is the request returned when no operation is needed
var NoOperation = &pb.TransactionRequest{}

// BallotNumber Utilities

// compareBallotNumbers compares two ballot numbers
func compareBallotNumbers(x *pb.BallotNumber, y *pb.BallotNumber) int {
	if x.N == y.N {
		return cmp.Compare(x.NodeID, y.NodeID)
	}
	return cmp.Compare(x.N, y.N)
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
			if msg, exists := acceptedMessagesMap[sequenceNum]; !exists || compareBallotNumbers(acceptedMessage.B, msg.B) == 1 {
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
