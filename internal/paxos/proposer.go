package paxos

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// Proposer structure handles proposer logic for leader node
type Proposer struct {
	id     string
	state  *ServerState
	config *ServerConfig
	peers  map[string]*models.Node

	// Components
	logger       *Logger
	checkpointer *CheckpointManager

	// Channels and context
	executionTriggerCh  chan ExecuteRequest
	installCheckpointCh chan int64
	ctx                 context.Context
	cancel              context.CancelFunc
}

// HandleTransactionRequest handles the transaction request and returns the sequence number
func (p *Proposer) HandleTransactionRequest(req *pb.TransactionRequest) error {
	// Assign a sequence number and create a record if it doesn't exist
	sequenceNum, _ := p.state.AssignSequenceNumberAndCreateRecord(p.state.GetBallotNumber(), req)
	log.Infof("[Proposer] Assigned sequence number %d for request %s", sequenceNum, utils.TransactionRequestString(req))

	// Run accept phase and set accepted flag
	if !p.state.StateLog.IsCommitted(sequenceNum) {
		accepted, err := p.RunAcceptPhase(sequenceNum, p.state.StateLog.GetRequest(sequenceNum))
		if err != nil {
			log.Warnf("[Proposer] Accept phase failed for request %s: %v", utils.TransactionRequestString(req), err)
			return err
		}
		if !accepted {
			log.Warnf("[Proposer] Accept phase failed for request %s: insufficient quorum", utils.TransactionRequestString(req))
			return errors.New("insufficient quorum")
		}
	}

	// Run commit phase and set committed flag
	err := p.RunCommitPhase(sequenceNum, p.state.StateLog.GetRequest(sequenceNum))

	// Send checkpoint message if sequence number is a multiple of k and purge
	if sequenceNum%p.config.K == 0 {
		digest := p.checkpointer.GetCheckpoint(sequenceNum).Digest
		p.SendCheckpointMessage(sequenceNum, digest)

		p.checkpointer.GetCheckpointPurgeRoutineCh() <- sequenceNum
	}

	return err
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
		signalCh := make(chan bool, 1)
		p.executionTriggerCh <- ExecuteRequest{
			SequenceNum: sequenceNum,
			SignalCh:    signalCh,
		}
		<-signalCh
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

// SendCheckpointMessage sends a checkpoint message to all peers
func (p *Proposer) SendCheckpointMessage(sequenceNum int64, digest []byte) {
	// Create checkpoint message
	checkpointMessage := &pb.CheckpointMessage{
		SequenceNum: sequenceNum,
		Digest:      digest,
	}

	// Logger: Add sent checkpoint message
	p.logger.AddSentCheckpointMessage(checkpointMessage)

	// Add checkpoint message to checkpoint message log
	p.checkpointer.AddCheckpointMessage(sequenceNum, checkpointMessage)

	// Multicast checkpoint message to all peers
	log.Infof("[SendCheckpointMessage] Sending checkpoint message for sequence number %d to all peers", sequenceNum)
	for _, peer := range p.peers {
		go func(peer *models.Node) {
			_, err := (*peer.Client).CheckpointRequest(context.Background(), checkpointMessage)
			if err != nil {
				log.Warnf("[SendCheckpointMessage] Failed to send checkpoint message to peer %s: %v", peer.ID, err)
				return
			}
		}(peer)
	}
}

// RunNewViewPhase runs the new view phase
func (p *Proposer) RunNewViewPhase(checkpointedSequenceNum int64, acceptMessages []*pb.AcceptMessage) {
	// Handle checkpoint
	if checkpointedSequenceNum > p.state.GetLastExecutedSequenceNum() {
		// If checkpoint is greater than executed sequence number, trigger install checkpoint routine

		// Get checkpoint from other nodes
		checkpoint, err := p.checkpointer.SendGetCheckpointRequest(checkpointedSequenceNum)
		if err != nil {
			log.Warnf("[RunNewViewPhase] Failed to get checkpoint for sequence number %d: %v", checkpointedSequenceNum, err)
			return
		}
		p.checkpointer.AddCheckpoint(checkpointedSequenceNum, checkpoint.Snapshot)
		p.installCheckpointCh <- checkpointedSequenceNum
	} else if checkpointedSequenceNum > p.state.GetLastCheckpointedSequenceNum() {
		// Trigger checkpoint purge routine
		p.checkpointer.GetCheckpointPurgeRoutineCh() <- checkpointedSequenceNum
	}

	// Update state with new accept messages
	for _, acceptMessage := range acceptMessages {
		p.state.StateLog.CreateRecordIfNotExists(acceptMessage.B, acceptMessage.SequenceNum, acceptMessage.Message)
		p.state.StateLog.SetAccepted(acceptMessage.SequenceNum)
	}
	// Set leader
	p.state.SetLeader(p.id)

	// Create new view message
	newViewMessage := &pb.NewViewMessage{
		B:           p.state.GetBallotNumber(),
		SequenceNum: checkpointedSequenceNum,
		AcceptLog:   acceptMessages,
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
func CreateProposer(id string, state *ServerState, config *ServerConfig, peers map[string]*models.Node, logger *Logger, checkpointer *CheckpointManager, executionTriggerCh chan ExecuteRequest, installCheckpointCh chan int64) *Proposer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Proposer{
		id:                  id,
		state:               state,
		config:              config,
		peers:               peers,
		logger:              logger,
		checkpointer:        checkpointer,
		executionTriggerCh:  executionTriggerCh,
		installCheckpointCh: installCheckpointCh,
		ctx:                 ctx,
		cancel:              cancel,
	}
}
