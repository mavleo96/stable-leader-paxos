package paxos

import (
	"context"
	"fmt"
	"sort"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// PrintLog prints the accept log, last reply timestamp, and accepted messages
func (s *PaxosServer) PrintLog(ctx context.Context, req *wrapperspb.Int64Value) (*emptypb.Empty, error) {
	log.Infof("Printing log command received")

	// TODO: Replace with actual test set number
	fmt.Println("LOGS FOR TEST SET:", req.Value)

	fmt.Println("Sent Prepare Messages:")
	for _, message := range s.logger.GetSentPrepareMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Received Prepare Messages:")
	for _, message := range s.logger.GetReceivedPrepareMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Sent Ack Messages:")
	for _, message := range s.logger.GetSentAckMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Received Ack Messages:")
	for _, message := range s.logger.GetReceivedAckMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Sent Accept Messages:")
	for _, message := range s.logger.GetSentAcceptMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Received Accept Messages:")
	for _, message := range s.logger.GetReceivedAcceptMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Sent Accepted Messages:")
	for _, message := range s.logger.GetSentAcceptedMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Received Accepted Messages:")
	for _, message := range s.logger.GetReceivedAcceptedMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Sent Commit Messages:")
	for _, message := range s.logger.GetSentCommitMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Received Commit Messages:")
	for _, message := range s.logger.GetReceivedCommitMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Received Transaction Requests:")
	for _, message := range s.logger.GetReceivedTransactionRequests() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Forwarded Transaction Requests:")
	for _, message := range s.logger.GetForwardedTransactionRequests() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Sent Transaction Responses:")
	for _, message := range s.logger.GetSentTransactionResponses() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	return &emptypb.Empty{}, nil
}

// PrintDB prints the database
func (s *PaxosServer) PrintDB(ctx context.Context, req *wrapperspb.Int64Value) (*emptypb.Empty, error) {
	log.Infof("Print database command received")

	fmt.Println("DATABASE FOR TEST SET:", req.Value)
	dbState, err := s.executor.db.GetDBState()
	if err != nil {
		log.Fatal(err)
	}

	// Sort client ids
	clientIDs := utils.Keys(dbState)
	sort.Strings(clientIDs)

	// Print by client id
	for _, clientID := range clientIDs {
		fmt.Printf("Balance: %s: %d\n", clientID, dbState[clientID])
	}
	fmt.Println("")
	return &emptypb.Empty{}, nil
}

// PrintStatus prints the status of the server
func (s *PaxosServer) PrintStatus(ctx context.Context, req *pb.StatusRequest) (*emptypb.Empty, error) {
	log.Infof("Print status command received")
	fmt.Println("STATUS FOR TEST SET:", req.TestSet)

	printRange := []int64{req.SequenceNum}
	if req.SequenceNum == 0 {
		printRange = utils.Range(1, s.state.StateLog.MaxSequenceNum()+1)
	}

	for _, i := range printRange {
		if !s.state.StateLog.Exists(i) {
			fmt.Println(s.state.StateLog.GetLogString(i), "nil")
			continue
		}
		fmt.Println(s.state.StateLog.GetLogString(i))
	}
	fmt.Println("")

	return &emptypb.Empty{}, nil
}

// PrintView prints the view of the server
func (s *PaxosServer) PrintView(ctx context.Context, req *wrapperspb.Int64Value) (*emptypb.Empty, error) {
	log.Infof("Print view command received")

	fmt.Println("NEW VIEW MESSAGES FOR TEST SET:", req.Value)

	fmt.Println("Sent new view messages:")
	for _, message := range s.logger.GetSentNewViewMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	fmt.Println("Received new view messages:")
	for _, message := range s.logger.GetReceivedNewViewMessages() {
		fmt.Println(message.String())
	}
	fmt.Println("")

	return &emptypb.Empty{}, nil
}
