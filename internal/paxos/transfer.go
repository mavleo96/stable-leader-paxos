package paxos

import (
	"context"
	"time"

	"github.com/mavleo96/cft-mavleo96/internal/models"
	"github.com/mavleo96/cft-mavleo96/internal/utils"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TransferRequest is the main function that rpc server calls to handle the transaction request
func (s *PaxosServer) TransferRequest(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	if !s.IsAlive {
		log.Warnf("Node is dead")
		return nil, status.Errorf(codes.Unavailable, "node is dead")
	}

	// System is not initialized yet
	if !s.SysInitialized {
		s.InitializeSystem()
	}

	// Forward request if not leader
	s.State.Mutex.Lock()
	defer s.State.Mutex.Unlock()
	if s.State.Leader.ID != s.NodeID {
		log.Warnf("Not leader, forwarding to leader %s", s.State.Leader.ID)
		go s.ForwardToLeader(req, s.State.Leader)
		return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "not leader")
	}

	// Duplicate requests with same timestamp are not ignored since the reply could have been lost or sent to backup node
	lastReply := s.LastReply[req.Sender]
	if lastReply != nil && req.Timestamp == lastReply.Timestamp {
		return s.LastReply[req.Sender], nil
	}
	// Older timestamp requests are ignored
	if lastReply != nil && req.Timestamp < lastReply.Timestamp {
		log.Warnf("Ignored %s; Last reply timestamp %d", utils.TransactionRequestString(req), s.LastReply[req.Sender].Timestamp)
		return UnsuccessfulTransactionResponse, status.Errorf(codes.AlreadyExists, "old timestamp")
	}

	// ACCEPT REQUEST LOGIC
	// Check if the request is already accepted
	sequenceNum, ok := GetSequenceNumberIfExistsInAcceptLog(s.State.AcceptLog, req)
	// If new request then assign a higher sequence number and accept it immediately before sending accepts
	if !ok {
		sequenceNum = MaxSequenceNumber(s.State.AcceptLog) + 1
		log.Infof("Accepted %s", utils.TransactionRequestString(req))
		s.State.AcceptLog[sequenceNum] = &pb.AcceptRecord{
			AcceptedBallotNum:   s.State.PromisedBallotNum,
			AcceptedSequenceNum: sequenceNum,
			AcceptedVal:         req,
			Committed:           false,
			Executed:            false,
			Result:              false,
		}
	}

	// COMMIT REQUEST LOGIC
	// Check if the request is already committed
	if !s.State.AcceptLog[sequenceNum].Committed {
		// Retry accept request until quorum of accepts is received
		s.State.SentAcceptMessages = append(s.State.SentAcceptMessages, &pb.AcceptMessage{
			B:           s.State.PromisedBallotNum,
			SequenceNum: sequenceNum,
			Message:     s.State.AcceptLog[sequenceNum].AcceptedVal,
		})
		ok, rejected, err := s.SendAcceptRequest(&pb.AcceptMessage{
			B:           s.State.PromisedBallotNum,
			SequenceNum: sequenceNum,
			Message:     s.State.AcceptLog[sequenceNum].AcceptedVal,
		})
		if err != nil {
			log.Fatal(err)
		}
		if rejected {
			log.Warnf("Stepping down since failed to get quorum of accepts for request %s", utils.TransactionRequestString(req))
			s.State.Leader = &models.Node{ID: ""}
			log.Warnf("Leader set to %s", s.State.Leader.ID)
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "accept request failed due to rejection")
		}
		if !ok {
			log.Warnf("Failed to get quorum of accepts for request %s", utils.TransactionRequestString(req))
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "accept request failed insufficient quorum")
		}

		// Once quorum of accepts received, commit it immediately and multicast the commit request
		s.State.AcceptLog[sequenceNum].Committed = true
		log.Infof("Committed %s", utils.TransactionRequestString(req))

		log.Infof("Multicasting commit request for sequence number %d", sequenceNum)
		err = s.SendCommitRequest(&pb.CommitMessage{
			B:           s.State.PromisedBallotNum,
			SequenceNum: sequenceNum,
			Transaction: req,
		})
		s.State.SentCommitMessages = append(s.State.SentCommitMessages, &pb.CommitMessage{
			B:           s.State.PromisedBallotNum,
			SequenceNum: sequenceNum,
			Transaction: req,
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	// EXECUTE REQUEST LOGIC
	// Try to execute the request and return the result
	log.Info("Trying to execute request")
	result, err := s.TryExecute(sequenceNum)
	if err != nil {
		return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "execute request failed try again")
	}
	s.State.AcceptLog[sequenceNum].Executed = true
	s.State.AcceptLog[sequenceNum].Result = result
	log.Infof("Executed request %s", utils.TransactionRequestString(req))

	response := &pb.TransactionResponse{
		B:         s.State.PromisedBallotNum,
		Timestamp: req.Timestamp,
		Sender:    req.Sender,
		Result:    result,
	}
	s.LastReply[req.Sender] = response
	return response, nil
}

// ForwardToLeader forwards the request to the leader
func (s *PaxosServer) ForwardToLeader(req *pb.TransactionRequest, leader *models.Node) {
	timerCtx := s.PaxosTimer.IncrementWaitCountOrStartAndGetContext()
	ctx, cancel := context.WithCancel(timerCtx)
	defer cancel()

	// Forward request to leader
	responseChan := make(chan error)
	go func(ctx context.Context, responseChan chan error, leader *models.Node) {
		defer close(responseChan)
		if leader.ID == "" {
			responseChan <- status.Errorf(codes.Aborted, "leader is empty")
			return
		}
		// Initialize connection to leader
		conn, err := grpc.NewClient(leader.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Warnf("Failed to connect to leader %s: %s", leader.ID, err)
			responseChan <- err
			return
		}
		defer conn.Close()
		leaderClient := pb.NewPaxosClient(conn)

		// Forward request to leader
		_, err = leaderClient.TransferRequest(ctx, req)
		if err != nil {
			responseChan <- err
			return
		}
		responseChan <- nil
	}(ctx, responseChan, leader)

	// Wait for response from leader or timer to cancel context
	select {
	case <-ctx.Done():
		log.Warnf("Backup timer expired, stopping leader client at %d", time.Now().UnixMilli())
		return
	case err := <-responseChan:
		if err != nil {
			log.Warnf("Error from leader on forwarding request %s", err)
		} else {
			s.PaxosTimer.DecrementContextWaitCountAndResetOrStopIfZero()
		}
		return
	}
}
