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
