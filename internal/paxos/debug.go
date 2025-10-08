package paxos

import (
	"context"
	"fmt"

	"github.com/mavleo96/cft-mavleo96/internal/utils"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PrintLog prints the accept log, last reply timestamp, and accepted messages
// TODO: need to refactor this later
func (s *PaxosServer) PrintLog(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	fmt.Println("Printing log")
	for _, record := range s.State.AcceptLog {
		fmt.Printf("Accept log: %v\n", utils.AcceptRecordString(record))
	}

	for clientID, timestamp := range s.LastReplyTimestamp {
		fmt.Printf("Last reply timestamp: <%s, %d>\n", clientID, timestamp)
	}
	for _, record := range s.AcceptedMessages {
		fmt.Printf("Accepted messages: %v\n", utils.AcceptedMessageString(record))
	}
	return &emptypb.Empty{}, nil
}

// PrintDB prints the database
// TODO: need to refactor this later
func (s *PaxosServer) PrintDB(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	fmt.Println("Printing database")
	s.DB.PrintDB()
	return &emptypb.Empty{}, nil
}

// PrintTimerState prints the timer state
func (s *PaxosServer) PrintTimerState(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	waitCount, running := s.PaxosTimer.GetTimerState()
	fmt.Printf("Timer state: %d, %t\n", waitCount, running)
	return &emptypb.Empty{}, nil
}
