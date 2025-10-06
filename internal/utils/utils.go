package utils

import (
	"fmt"

	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
)

func TransactionResponseString(resp *pb.TransactionResponse) string {
	return fmt.Sprintf(
		"<REPLY, %s, %d, %s, %t>",
		BallotNumberString(resp.B),
		resp.Timestamp,
		resp.Sender,
		resp.Result,
	)
}

func AcceptedMessageString(record *pb.AcceptedMessage) string {
	return fmt.Sprintf(
		"<ACCEPTED, %s, %d, %s, %s>",
		BallotNumberString(record.B),
		record.SequenceNum,
		TransactionRequestString(record.Message),
		record.AcceptorID,
	)
}

func AcceptRecordString(record *pb.AcceptRecord) string {
	return fmt.Sprintf(
		"<ACCEPT, %s, %d, %s>",
		BallotNumberString(record.AcceptedBallotNumber),
		record.AcceptedSequenceNumber,
		TransactionRequestString(record.AcceptedVal),
	)
}

func TransactionRequestString(req *pb.TransactionRequest) string {
	return fmt.Sprintf(
		"<REQUEST, %s, %d, %s>",
		TransactionString(req.Transaction),
		req.Timestamp,
		req.Sender,
	)
}

func BallotNumberString(ballot *pb.BallotNumber) string {
	return fmt.Sprintf("(%s, %d)", ballot.NodeID, ballot.N)
}

func TransactionString(req *pb.Transaction) string {
	return fmt.Sprintf("(%s, %s, %d)", req.Sender, req.Receiver, req.Amount)
}
