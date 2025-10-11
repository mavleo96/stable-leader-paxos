package utils

import (
	"fmt"

	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
)

func TransactionResponseString(x *pb.TransactionResponse) string {
	return fmt.Sprintf(
		"<REPLY, %s, %d, %s, %t>",
		BallotNumberString(x.B),
		x.Timestamp,
		x.Sender,
		x.Result,
	)
}

// func AcceptedMessageString(x *pb.AcceptedMessage) string {
// 	return fmt.Sprintf(
// 		"<ACCEPTED, %s, %d, %s, %s>",
// 		BallotNumberString(x.B),
// 		x.SequenceNum,
// 		TransactionRequestString(x.Message),
// 		x.AcceptorID,
// 	)
// }

func PrintLogString(x *pb.AcceptRecord) string {
	return fmt.Sprintf(
		"%s, <Committed: %t, Executed: %t, Result: %t>",
		AcceptRecordString(x),
		x.Committed,
		x.Executed,
		x.Result,
	)
}

func AcceptRecordString(x *pb.AcceptRecord) string {
	return fmt.Sprintf(
		"<ACCEPT, %s, %d, %s>",
		BallotNumberString(x.AcceptedBallotNum),
		x.AcceptedSequenceNum,
		TransactionRequestString(x.AcceptedVal),
	)
}

func TransactionRequestString(x *pb.TransactionRequest) string {
	return fmt.Sprintf(
		"<REQUEST, %s, %d, %s>",
		TransactionString(x.Transaction),
		x.Timestamp,
		x.Sender,
	)
}

func BallotNumberString(x *pb.BallotNumber) string {
	return fmt.Sprintf("<%d, %s>", x.N, x.NodeID)
}

func TransactionString(x *pb.Transaction) string {
	return fmt.Sprintf("(%s, %s, %d)", x.Sender, x.Receiver, x.Amount)
}
