package paxos

import (
	"crypto/rand"
	"math/big"
	"time"

	pb "github.com/mavleo96/stable-leader-paxos/pb/paxos"
)

// BallotNumber Utilities

// BallotNumberIsHigherOrEqual checks if current >= new
func BallotNumberIsHigherOrEqual(current *pb.BallotNumber, new *pb.BallotNumber) bool {
	if new.N == current.N {
		return new.NodeID >= current.NodeID
	}
	return new.N >= current.N
}

// BallotNumberIsHigher checks if current > new
func BallotNumberIsHigher(current *pb.BallotNumber, new *pb.BallotNumber) bool {
	if new.N == current.N {
		return new.NodeID > current.NodeID
	}
	return new.N > current.N
}

// AcceptLog Utilities

// GetSequenceNumberIfExistsInAcceptLog gets the sequence number from the accept log
func GetSequenceNumberIfExistsInAcceptLog(acceptLog map[int64]*pb.AcceptRecord, req *pb.TransactionRequest) (int64, bool) {
	for _, record := range acceptLog {
		if record.AcceptedVal.Sender == req.Sender && record.AcceptedVal.Timestamp == req.Timestamp {
			return record.AcceptedSequenceNum, true
		}
	}
	return 0, false
}

// MaxSequenceNumber gets the max sequence number from the accept log
func MaxSequenceNumber(acceptLog map[int64]*pb.AcceptRecord) int64 {
	max := int64(0)
	for _, record := range acceptLog {
		if record.AcceptedSequenceNum > max {
			max = record.AcceptedSequenceNum
		}
	}
	return max
}

// Timer Utilities

// RandomTimeout returns a random timeout between min and max
func RandomTimeout(min time.Duration, max time.Duration) (time.Duration, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(max.Milliseconds()-min.Milliseconds()))
	return time.Duration(n.Int64()+min.Milliseconds()) * time.Millisecond, err
}
