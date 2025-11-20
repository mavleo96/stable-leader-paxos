package paxos

import (
	"context"
	"errors"
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
}

// HandleTransactionRequest handles the transaction request and returns the sequence number
func (p *Proposer) HandleTransactionRequest(req *pb.TransactionRequest) int64 {
	// Assign a sequence number and create a record if it doesn't exist
	sequenceNum, _ := p.state.StateLog.AssignSequenceNumberAndCreateRecord(p.state.GetBallotNumber(), req)
	log.Infof("[Proposer] Assigned sequence number %d for request %s", sequenceNum, utils.TransactionRequestString(req))

	// go p.RunProtocol(sequenceNum)

	return sequenceNum
}

// RunProtocol runs the paxos protocol for a given sequence number
func (p *Proposer) RunProtocol(sequenceNum int64) error {
	log.Infof("[Proposer] Running protocol for sequence number %d", sequenceNum)
	// Run accept phase and set accepted flag
	p.state.StateLog.SetAccepted(sequenceNum)
	acceptMessage := &pb.AcceptMessage{
		B:           p.state.GetBallotNumber(),
		SequenceNum: sequenceNum,
		Message:     p.state.StateLog.GetRequest(sequenceNum),
	}
	// Logger: Add sent accept message
	p.logger.AddSentAcceptMessage(acceptMessage)
	accepted, err := p.RunAcceptPhase(acceptMessage)
	if err != nil {
		return err
	}
	if !accepted {
		return errors.New("accept request failed insufficient quorum")
	}

	// If accepted, set committed flag and run commit phase
	p.state.StateLog.SetCommitted(sequenceNum)
	commitMessage := &pb.CommitMessage{
		B:           p.state.GetBallotNumber(),
		SequenceNum: sequenceNum,
		Message:     p.state.StateLog.GetRequest(sequenceNum),
	}
	// Logger: Add sent commit message
	p.logger.AddSentCommitMessage(commitMessage)
	go p.BroadcastCommitRequest(commitMessage)

	// If not executed, trigger execution
	if !p.state.StateLog.IsExecuted(sequenceNum) {
		p.executionTriggerCh <- sequenceNum
	}

	return nil
}

// RunAcceptPhase sends an accept request to all peers and returns the response from each peer
func (p *Proposer) RunAcceptPhase(req *pb.AcceptMessage) (bool, error) {
	// Multicast accept request to all peers
	wg := sync.WaitGroup{}
	responseCh := make(chan bool, len(p.peers))
	log.Infof("[Proposer] Running accept phase for request %s", utils.TransactionRequestString(req.Message))
	for _, peer := range p.peers {
		wg.Add(1)
		go func(peer *models.Node, responseCh chan bool) {
			defer wg.Done()
			resp, err := (*peer.Client).AcceptRequest(context.Background(), req)
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

// BroadcastCommitRequest sends a commit request to all peers
func (p *Proposer) BroadcastCommitRequest(req *pb.CommitMessage) error {
	log.Infof("[Proposer] Broadcasting commit request %s", utils.TransactionRequestString(req.Message))
	for _, peer := range p.peers {
		go func(peer *models.Node) {
			_, err := (*peer.Client).CommitRequest(context.Background(), req)
			if err != nil {
				log.Warn(err)
				return
			}
		}(peer)
	}
	return nil
}

// CreateProposer creates a new proposer
func CreateProposer(id string, state *ServerState, config *ServerConfig, peers map[string]*models.Node, executionTriggerCh chan int64, logger *Logger) *Proposer {
	return &Proposer{
		id:                 id,
		state:              state,
		config:             config,
		peers:              peers,
		executionTriggerCh: executionTriggerCh,
		logger:             logger,
	}
}
