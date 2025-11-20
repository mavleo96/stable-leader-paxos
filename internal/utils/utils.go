package utils

import (
	"fmt"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

func TransactionResponseString(x *pb.TransactionResponse) string {
	if x == nil {
		return "<REPLY, nil>"
	}
	return fmt.Sprintf(
		"<REPLY, %s, %d, %s, %t>",
		BallotNumberString(x.B),
		x.Timestamp,
		x.Sender,
		x.Result,
	)
}

func AcceptedMessageString(x *pb.AcceptedMessage) string {
	if x == nil {
		return "<ACCEPTED, nil>"
	}
	return fmt.Sprintf(
		"<ACCEPTED, %s, %d, %s, %s>",
		BallotNumberString(x.B),
		x.SequenceNum,
		TransactionRequestString(x.Message),
		x.NodeID,
	)
}

func TransactionRequestString(x *pb.TransactionRequest) string {
	if x == nil {
		return "<REQUEST, nil>"
	}
	return fmt.Sprintf(
		"<REQUEST, %s, %d, %s>",
		TransactionString(x.Transaction),
		x.Timestamp,
		x.Sender,
	)
}

func BallotNumberString(x *pb.BallotNumber) string {
	if x == nil {
		return "<0, nil>"
	}
	return fmt.Sprintf("<%d, %s>", x.N, x.NodeID)
}

func TransactionString(x *pb.Transaction) string {
	if x == nil {
		return "(nil)"
	}
	return fmt.Sprintf("(%s, %s, %d)", x.Sender, x.Receiver, x.Amount)
}
