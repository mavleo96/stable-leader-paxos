package paxos

import (
	"context"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PrintLog prints the accept log, last reply timestamp, and accepted messages
// TODO: need to refactor this later
func (s *PaxosServer) PrintLog(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	log.Info("Printing log")
	for _, record := range s.State.AcceptLog {
		log.Infof("Accept log: %v", record)
	}

	for _, record := range s.LastReplyTimestamp {
		log.Infof("Last reply timestamp: %v", record)
	}
	for _, record := range s.AcceptedMessages {
		log.Infof("Accepted messages: %v", record)
	}
	return &emptypb.Empty{}, nil
}

// PrintDB prints the database
// TODO: need to refactor this later
func (s *PaxosServer) PrintDB(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	log.Info("Printing database")
	s.DB.PrintDB()
	return &emptypb.Empty{}, nil
}
