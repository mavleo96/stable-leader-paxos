package paxos

import (
	"context"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TransferRequest is the main function that rpc server calls to handle the transaction request
func (s *PaxosServer) TransferRequest(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	if !s.config.Alive {
		log.Warnf("Node is dead")
		return nil, status.Errorf(codes.Unavailable, "node is dead")
	}

	// System is not initialized yet
	if !s.SysInitialized {
		s.InitializeSystem()
	}

	// Forward request if not leader
	if s.state.GetLeader() != s.NodeID {
		log.Warnf("Not leader, forwarding to leader %s", s.state.GetLeader())
		go s.ForwardToLeader(req, s.state.GetLeader())
		return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "not leader")
	}

	// Duplicate requests with same timestamp are not ignored since the reply could have been lost or sent to backup node
	lastReply := s.state.LastReply.Get(req.Sender)
	if lastReply != nil && req.Timestamp == lastReply.Timestamp {
		return s.state.LastReply.Get(req.Sender), nil
	}
	// Older timestamp requests are ignored
	if lastReply != nil && req.Timestamp < lastReply.Timestamp {
		log.Warnf("Ignored %s; Last reply timestamp %d", utils.TransactionRequestString(req), s.state.LastReply.Get(req.Sender).Timestamp)
		return UnsuccessfulTransactionResponse, status.Errorf(codes.AlreadyExists, "old timestamp")
	}

	// ACCEPT REQUEST LOGIC
	// Check if the request is already accepted
	sequenceNum, created := s.state.StateLog.AssignSequenceNumberAndCreateRecord(s.state.GetBallotNumber(), req)
	// If new request then assign a higher sequence number and accept it immediately before sending accepts
	if created {
		log.Infof("Accepted %s", utils.TransactionRequestString(req))
		s.state.StateLog.SetAccepted(sequenceNum)
	}

	// COMMIT REQUEST LOGIC
	// Check if the request is already committed
	if !s.state.StateLog.IsCommitted(sequenceNum) {
		// Retry accept request until quorum of accepts is received
		acceptMessage := &pb.AcceptMessage{
			B:           s.state.GetBallotNumber(),
			SequenceNum: sequenceNum,
			Message:     s.state.StateLog.GetRequest(sequenceNum),
		}
		s.logger.AddSentAcceptMessage(acceptMessage)
		ok, rejected, err := s.SendAcceptRequest(acceptMessage)
		if err != nil {
			log.Fatal(err)
		}
		if rejected {
			log.Warnf("Stepping down since failed to get quorum of accepts for request %s", utils.TransactionRequestString(req))
			// s.State.Leader = &models.Node{ID: ""}
			// log.Warnf("Leader set to %s", s.State.Leader.ID)
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "accept request failed due to rejection")
		}
		if !ok {
			log.Warnf("Failed to get quorum of accepts for request %s", utils.TransactionRequestString(req))
			return UnsuccessfulTransactionResponse, status.Errorf(codes.Aborted, "accept request failed insufficient quorum")
		}

		// Once quorum of accepts received, commit it immediately and multicast the commit request
		s.state.StateLog.SetCommitted(sequenceNum)
		log.Infof("Committed %s", utils.TransactionRequestString(req))

		log.Infof("Multicasting commit request for sequence number %d", sequenceNum)
		commitMessage := &pb.CommitMessage{
			B:           s.state.GetBallotNumber(),
			SequenceNum: sequenceNum,
			Transaction: req,
		}
		s.logger.AddSentCommitMessage(commitMessage)

		err = s.SendCommitRequest(commitMessage)
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
	s.state.StateLog.SetExecuted(sequenceNum)
	s.state.StateLog.SetResult(sequenceNum, utils.BoolToInt64(result))
	log.Infof("Executed request %s", utils.TransactionRequestString(req))

	response := &pb.TransactionResponse{
		B:         s.state.GetBallotNumber(),
		Timestamp: req.Timestamp,
		Sender:    req.Sender,
		Result:    result,
	}
	s.state.LastReply.Update(req.Sender, response)
	return response, nil
}

// ForwardToLeader forwards the request to the leader
func (s *PaxosServer) ForwardToLeader(req *pb.TransactionRequest, leader string) {
	timerCtx := s.PaxosTimer.IncrementWaitCountOrStartAndGetContext()
	ctx, cancel := context.WithCancel(timerCtx)
	defer cancel()

	// Forward request to leader
	responseChan := make(chan error)
	go func(ctx context.Context, responseChan chan error, leaderID string) {
		defer close(responseChan)
		if leaderID == "" {
			responseChan <- status.Errorf(codes.Aborted, "leader is empty")
			return
		}
		// Initialize connection to leader
		conn, err := grpc.NewClient(s.Peers[leaderID].Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Warnf("Failed to connect to leader %s: %s", leaderID, err)
			responseChan <- err
			return
		}
		defer conn.Close()
		leaderClient := pb.NewPaxosNodeClient(conn)

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
