package paxos

import (
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
)

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
