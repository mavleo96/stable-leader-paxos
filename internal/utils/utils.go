package utils

import (
	"fmt"

	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
)

func TransactionRequestString(req *pb.TransactionRequest) string {
	return fmt.Sprintf("((%s, %s, %d), %d)", req.Transaction.Sender, req.Transaction.Receiver, req.Transaction.Amount, req.Timestamp)
}
