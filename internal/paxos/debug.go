package paxos

import (
	"context"
	"fmt"
	"sort"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// PrintLog prints the accept log, last reply timestamp, and accepted messages
func (s *PaxosServer) PrintLog(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	fmt.Println("Printing Current Accept Log:")
	sequenceNum := MaxSequenceNumber(s.State.AcceptLog)
	for i := int64(0); i <= sequenceNum; i++ {
		record, ok := s.State.AcceptLog[i]
		if ok {
			fmt.Printf("%v\n", utils.PrintLogString(record))
		}
	}
	fmt.Println("")
	fmt.Println("Printing Sent Accept Log:")
	for _, record := range s.State.SentAcceptMessages {
		fmt.Printf("%v\n", record.String())
	}
	fmt.Println("")
	fmt.Println("Printing Received Accept Log:")
	for _, record := range s.State.ReceivedAcceptMessages {
		fmt.Printf("%v\n", record.String())
	}
	fmt.Println("")
	fmt.Println("Printing Sent Prepare Log:")
	for _, record := range s.State.SentPrepareMessages {
		fmt.Printf("%v\n", record.String())
	}
	fmt.Println("")
	fmt.Println("Printing Received Prepare Log:")
	for _, record := range s.State.ReceivedPrepareMessages {
		fmt.Printf("%v\n", record.String())
	}
	fmt.Println("")
	fmt.Println("Printing Sent Commit Log:")
	for _, record := range s.State.SentCommitMessages {
		fmt.Printf("%v\n", record.String())
	}
	fmt.Println("")
	fmt.Println("Printing Received Commit Log:")
	for _, record := range s.State.ReceivedCommitMessages {
		fmt.Printf("%v\n", record.String())
	}
	fmt.Println("")
	fmt.Println("Printing Sent Accepted Log:")
	for _, record := range s.State.SentAcceptedMessages {
		fmt.Printf("%v\n", record.String())
	}
	fmt.Println("")
	fmt.Println("Printing Received Accepted Log:")
	for _, record := range s.State.ReceivedAcceptedMessages {
		fmt.Printf("%v\n", record.String())
	}
	fmt.Println("")
	return &emptypb.Empty{}, nil
}

// PrintDB prints the database
func (s *PaxosServer) PrintDB(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	fmt.Println("Printing database:")
	db_state, err := s.DB.PrintDB()
	if err != nil {
		log.Fatal(err)
	}

	// Sort client ids
	keys := make([]string, 0, len(db_state))
	for k := range db_state {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Print by client id
	for _, k := range keys {
		fmt.Printf("Balance: %s: %d\n", k, db_state[k])
	}
	fmt.Println("")
	return &emptypb.Empty{}, nil
}

func (s *PaxosServer) PrintStatus(ctx context.Context, req *wrapperspb.Int64Value) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	fmt.Println("Printing status:")
	record, ok := s.State.AcceptLog[req.Value]
	if !ok {
		fmt.Printf("Sequence Number: %d, Status: X\n", req.Value)
	} else if record.Executed {
		fmt.Printf("Sequence Number: %d, Status: E, Message: %s\n", req.Value, utils.TransactionRequestString(record.AcceptedVal))
	} else if record.Committed {
		fmt.Printf("Sequence Number: %d, Status: C, Message: %s\n", req.Value, utils.TransactionRequestString(record.AcceptedVal))
	} else {
		fmt.Printf("Sequence Number: %d, Status: A, Message: %s\n", req.Value, utils.TransactionRequestString(record.AcceptedVal))
	}
	fmt.Println("")
	return &emptypb.Empty{}, nil
}

func (s *PaxosServer) PrintView(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	s.State.Mutex.RLock()
	defer s.State.Mutex.RUnlock()
	fmt.Println("Printing view:")
	for _, record := range s.State.NewViewLog {
		if record == nil {
			continue
		}
		acceptLogString := ""
		for _, acceptRecord := range record.AcceptLog {
			acceptLogString += "\t" + utils.AcceptRecordString(acceptRecord) + "\n"
		}
		fmt.Printf("<NEW VIEW, %s, \n%s>\n", utils.BallotNumberString(record.B), acceptLogString)
	}
	fmt.Println("")
	return &emptypb.Empty{}, nil
}
