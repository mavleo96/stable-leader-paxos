package paxos

import (
	"context"
	"sync"

	paxospb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
)

type PaxosServer struct {
	NodeID string
	Addr   string
	State  AcceptorState
	paxospb.UnimplementedPaxosServer
}

type AcceptorState struct {
	Mutex             sync.Mutex
	PromisedBallotNum *paxospb.BallotNumber
	AcceptLog         []*paxospb.AcceptRecord // TODO: should this be a map?
}

func BallotNumberIsHigherOrEqual(current *paxospb.BallotNumber, new *paxospb.BallotNumber) bool {
	if new.N == current.N {
		return new.NodeID >= current.NodeID
	}
	return new.N >= current.N
}

func BallotNumberIsHigher(current *paxospb.BallotNumber, new *paxospb.BallotNumber) bool {
	if new.N == current.N {
		return new.NodeID > current.NodeID
	}
	return new.N > current.N
}

func (s *PaxosServer) PrepareRequest(ctx context.Context, req *paxospb.PrepareMessage) (*paxospb.AckMessage, error) {
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()

	// TODO: don't promise if timer has not expired -> need to log messages and then respond to highest

	if !BallotNumberIsHigher(s.State.PromisedBallotNum, req.B) {
		log.Warnf("Rejected prepare request %v", req.String())
		return &paxospb.AckMessage{Ok: false}, nil
	}

	s.State.PromisedBallotNum = req.B

	log.Infof("Accepted prepare request %v", req.String())
	return &paxospb.AckMessage{
		Ok:        true,
		AcceptNum: s.State.PromisedBallotNum,
		AcceptLog: s.State.AcceptLog,
	}, nil

}

func (s *PaxosServer) AcceptRequest(ctx context.Context, req *paxospb.AcceptMessage) (*paxospb.AcceptedMessage, error) {
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()
	// req.B.N

	// check if req ballot number is higher or equal to state
	// if no -> return ok = false
	if !BallotNumberIsHigherOrEqual(s.State.PromisedBallotNum, req.B) {
		log.Warnf("Rejected accept request %v", req.String())
		return &paxospb.AcceptedMessage{Ok: false, SequenceNum: req.SequenceNum, AcceptorID: s.NodeID}, nil
	}

	// update state
	s.State.PromisedBallotNum = req.B
	updated := false
	for i, acceptRecord := range s.State.AcceptLog {
		if acceptRecord.AcceptedSequenceNumber == req.SequenceNum {
			s.State.AcceptLog[i] = &paxospb.AcceptRecord{
				AcceptedBallotNumber:   req.B,
				AcceptedSequenceNumber: req.SequenceNum,
				AcceptedVal:            req.Message,
			}
			updated = true
			break
		}
	}
	if !updated {
		s.State.AcceptLog = append(s.State.AcceptLog, &paxospb.AcceptRecord{
			AcceptedBallotNumber:   req.B,
			AcceptedSequenceNumber: req.SequenceNum,
			AcceptedVal:            req.Message,
		})
	}

	// else yes -> accept
	log.Infof("Accepted accept request %v", req.String())
	return &paxospb.AcceptedMessage{
		Ok:          true,
		SequenceNum: req.SequenceNum,
		Message:     req.Message,
		AcceptorID:  s.NodeID,
	}, nil
}

func (s *PaxosServer) CommitRequest(ctx context.Context, req *paxospb.CommitMessage) (*emptypb.Empty, error) {
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()
	log.Infof("Commited %v", req.String())
	return &emptypb.Empty{}, nil
}
