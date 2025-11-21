package paxos

import (
	"context"
	"io"
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// Proposer structure handles proposer logic for leader node
type Proposer struct {
	id                 string
	state              *ServerState
	config             *ServerConfig
	peers              map[string]*models.Node
	executionTriggerCh chan int64
	logger             *Logger
	ctx                context.Context
	cancel             context.CancelFunc
}

// HandleTransactionRequest handles the transaction request and returns the sequence number
func (p *Proposer) HandleTransactionRequest(req *pb.TransactionRequest) {
	// Assign a sequence number and create a record if it doesn't exist
	sequenceNum, _ := p.state.StateLog.AssignSequenceNumberAndCreateRecord(p.state.GetBallotNumber(), req)
	log.Infof("[Proposer] Assigned sequence number %d for request %s", sequenceNum, utils.TransactionRequestString(req))

	// Run accept phase and set accepted flag
	if !p.state.StateLog.IsCommitted(sequenceNum) {
		accepted, err := p.RunAcceptPhase(sequenceNum, p.state.StateLog.GetRequest(sequenceNum))
		if err != nil {
			log.Warnf("[Proposer] Accept phase failed for request %s: %v", utils.TransactionRequestString(req), err)
			return
		}
		if !accepted {
			log.Warnf("[Proposer] Accept phase failed for request %s: insufficient quorum", utils.TransactionRequestString(req))
			return
		}
	}

	p.RunCommitPhase(sequenceNum, p.state.StateLog.GetRequest(sequenceNum))
}

// RunAcceptPhase sends an accept request to all peers and returns the response from each peer
func (p *Proposer) RunAcceptPhase(sequenceNum int64, req *pb.TransactionRequest) (bool, error) {
	// Set accepted flag and create accept message
	p.state.StateLog.SetAccepted(sequenceNum)
	acceptMessage := &pb.AcceptMessage{
		B:           p.state.GetBallotNumber(),
		SequenceNum: sequenceNum,
		Message:     req,
	}

	// Logger: Add sent accept message
	p.logger.AddSentAcceptMessage(acceptMessage)

	// Multicast accept request to all peers
	wg := sync.WaitGroup{}
	responseCh := make(chan bool, len(p.peers))
	log.Infof("[Proposer] Running accept phase for request %s", utils.TransactionRequestString(req))
	for _, peer := range p.peers {
		wg.Add(1)
		go func(peer *models.Node, responseCh chan bool) {
			defer wg.Done()
			resp, err := (*peer.Client).AcceptRequest(context.Background(), acceptMessage)
			if err != nil {
				log.Warn(err)
				responseCh <- false
				return
			}

			// Logger: Add received accepted message
			p.logger.AddReceivedAcceptedMessage(resp)

			responseCh <- true
		}(peer, responseCh)
	}
	go func() {
		wg.Wait()
		close(responseCh)
	}()

	// Wait for responses and check if quorum of accepts is reached
	acceptedCount := int64(1)
	for a := range responseCh {
		if a {
			acceptedCount++
		}
		if acceptedCount >= p.config.F+1 {
			return true, nil
		}
	}
	return false, nil
}

// RunCommitPhase commits the transaction request
func (p *Proposer) RunCommitPhase(sequenceNum int64, req *pb.TransactionRequest) error {
	// Set committed flag and create commit message
	p.state.StateLog.SetCommitted(sequenceNum)
	commitMessage := &pb.CommitMessage{
		B:           p.state.GetBallotNumber(),
		SequenceNum: sequenceNum,
		Message:     req,
	}

	// Logger: Add sent commit message
	p.logger.AddSentCommitMessage(commitMessage)

	// Broadcast commit request to all peers
	go p.BroadcastCommitRequest(commitMessage)

	// If not executed, trigger execution
	if !p.state.StateLog.IsExecuted(sequenceNum) {
		p.executionTriggerCh <- sequenceNum
	}

	return nil
}

// BroadcastCommitRequest sends a commit request to all peers
func (p *Proposer) BroadcastCommitRequest(commitMessage *pb.CommitMessage) error {
	log.Infof("[Proposer] Broadcasting commit request %s", utils.TransactionRequestString(commitMessage.Message))
	for _, peer := range p.peers {
		go func(peer *models.Node) {
			_, err := (*peer.Client).CommitRequest(context.Background(), commitMessage)
			if err != nil {
				log.Warn(err)
				return
			}
		}(peer)
	}
	return nil
}

// RunNewViewPhase runs the new view phase
func (p *Proposer) RunNewViewPhase(ackMessages []*pb.AckMessage) {
	currentBallotNumber := p.state.GetBallotNumber()
	acceptMessages := aggregateAckMessages(currentBallotNumber, ackMessages)

	// Update state with new accept messages
	for _, acceptMessage := range acceptMessages {
		p.state.StateLog.CreateRecordIfNotExists(acceptMessage.B, acceptMessage.SequenceNum, acceptMessage.Message)
		p.state.StateLog.SetAccepted(acceptMessage.SequenceNum)
	}

	// Create new view message
	newViewMessage := &pb.NewViewMessage{
		B:         currentBallotNumber,
		AcceptLog: acceptMessages,
	}

	// Logger: Add sent new view message
	p.logger.AddSentNewViewMessage(newViewMessage)

	// Multicast new view request to all peers
	wg := sync.WaitGroup{}
	responseCh := make(chan *pb.AcceptedMessage, 100)
	for _, peer := range p.peers {
		wg.Add(1)
		go func(peer *models.Node) {
			defer wg.Done()
			stream, err := (*peer.Client).NewViewRequest(context.Background(), newViewMessage)
			if err != nil {
				log.Warn(err)
				return
			}

			for {
				acceptedMessage, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Warn(err)
					return
				}

				// Logger: Add received accepted message
				p.logger.AddReceivedAcceptedMessage(acceptedMessage)

				responseCh <- acceptedMessage
			}
		}(peer)
	}
	go func() {
		wg.Wait()
		close(responseCh)
	}()

	// Wait for responses and check if quorum of accepts is reached
	acceptedCountMap := make(map[int64]int64, 100)
	for {
		acceptedMessage, ok := <-responseCh
		if !ok {
			break
		}

		sequenceNum := acceptedMessage.SequenceNum
		acceptedCountMap[sequenceNum]++
		if acceptedCountMap[sequenceNum] == p.config.F {
			go p.RunCommitPhase(sequenceNum, acceptedMessage.Message)
		}
	}
}

// CreateProposer creates a new proposer
func CreateProposer(id string, state *ServerState, config *ServerConfig, peers map[string]*models.Node, executionTriggerCh chan int64, logger *Logger) *Proposer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Proposer{
		id:                 id,
		state:              state,
		config:             config,
		peers:              peers,
		executionTriggerCh: executionTriggerCh,
		logger:             logger,
		ctx:                ctx,
		cancel:             cancel,
	}
}

// aggregateAckMessages aggregates the accepted messages from all ack messages
func aggregateAckMessages(newBallotNumber *pb.BallotNumber, ackMessages []*pb.AckMessage) []*pb.AcceptMessage {
	// Aggregate accepted messages from all ack messages
	acceptedMessagesMap := make(map[int64]*pb.AcceptedMessage, 100)
	maxSequenceNum := int64(0)
	for _, ackMessage := range ackMessages {
		for _, acceptedMessage := range ackMessage.AcceptLog {
			sequenceNum := acceptedMessage.SequenceNum
			if sequenceNum > maxSequenceNum {
				maxSequenceNum = sequenceNum
			}
			if msg, exists := acceptedMessagesMap[sequenceNum]; !exists || BallotNumberIsHigher(msg.B, acceptedMessage.B) {
				acceptedMessagesMap[sequenceNum] = acceptedMessage
			}
		}
	}

	// Created sorted accept messages; fill in the gaps with no-op messages
	sortedAcceptMessages := make([]*pb.AcceptMessage, maxSequenceNum)
	for sequenceNum := int64(1); sequenceNum <= maxSequenceNum; sequenceNum++ {
		if msg, exists := acceptedMessagesMap[sequenceNum]; exists {
			sortedAcceptMessages[sequenceNum-1] = &pb.AcceptMessage{
				B:           newBallotNumber,
				SequenceNum: sequenceNum,
				Message:     msg.Message,
			}
		} else {
			sortedAcceptMessages[sequenceNum-1] = &pb.AcceptMessage{
				B:           newBallotNumber,
				SequenceNum: sequenceNum,
				Message:     NoOperation,
			}
		}
	}
	return sortedAcceptMessages
}
