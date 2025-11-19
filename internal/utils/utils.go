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
		x.AcceptorID,
	)
}

func PrintLogString(x *pb.AcceptRecord) string {
	if x == nil {
		return "<LOG, nil>"
	}
	return fmt.Sprintf(
		"%s, <Committed: %t, Executed: %t, Result: %t>",
		AcceptRecordString(x),
		x.Committed,
		x.Executed,
		x.Result,
	)
}

func AcceptRecordString(x *pb.AcceptRecord) string {
	if x == nil {
		return "<ACCEPT, nil>"
	}
	return fmt.Sprintf(
		"<ACCEPT, %s, %d, %s>",
		BallotNumberString(x.AcceptedBallotNum),
		x.AcceptedSequenceNum,
		TransactionRequestString(x.AcceptedVal),
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
