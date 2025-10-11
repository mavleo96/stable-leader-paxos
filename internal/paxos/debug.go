package paxos

import (
	"context"
	"fmt"
	"sort"

	"github.com/mavleo96/cft-mavleo96/internal/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PrintLog prints the accept log, last reply timestamp, and accepted messages
// TODO: need to refactor this later
func (s *PaxosServer) PrintLog(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	fmt.Println("Printing log")
	sequenceNum := MaxSequenceNumber(s.State.AcceptLog)
	for i := int64(0); i <= sequenceNum; i++ {
		record, ok := s.State.AcceptLog[i]
		if ok {
			fmt.Printf("Accept log: %v\n", utils.PrintLogString(record))
		}
	}
	return &emptypb.Empty{}, nil
}

// PrintDB prints the database
// TODO: need to refactor this later
func (s *PaxosServer) PrintDB(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	fmt.Println("Printing database")
	db_state, err := s.DB.PrintDB()
	if err != nil {
		log.Fatal(err)
	}
	//TODO: print by client id
	keys := make([]string, 0, len(db_state))
	for k := range db_state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("Balance: %s: %d\n", k, db_state[k])
	}
	return &emptypb.Empty{}, nil
}

// PrintTimerState prints the timer state
func (s *PaxosServer) PrintTimerState(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	waitCount, running, timeout := s.PaxosTimer.GetTimerState()
	fmt.Printf("Timer state: %d, %t, %d\n", waitCount, running, timeout)
	return &emptypb.Empty{}, nil
}
