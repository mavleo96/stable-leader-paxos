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

// SendPrepareMessage sends a prepare request to all peers and sends the ack message to the response channel
func (p *Proposer) SendPrepareMessage(prepareMessage *pb.PrepareMessage, responseCh chan *pb.AckMessage) {
	// Logger: Add sent prepare message
	p.logger.AddSentPrepareMessage(prepareMessage)

	// Multicast prepare request to all peers
	wg := sync.WaitGroup{}
	log.Infof("[SendPrepareMessage] Sending prepare message %s", utils.LoggingString(prepareMessage.B))
	for _, peer := range p.peers {
		wg.Add(1)
		go func(peer *models.Node) {
			defer wg.Done()
			ackMessage, err := peer.Client.PrepareRequest(context.Background(), prepareMessage)
			if err != nil || ackMessage == nil {
				log.Warnf("[SendPrepareMessage] Failed to send prepare message to peer %s: %v", peer.ID, err)
				return
			}

			// Logger: Add received ack message
			p.logger.AddReceivedAckMessage(ackMessage)

			log.Infof("[SendPrepareMessage] Received ack message from %s: %s", peer.ID, ackMessage.String())
			responseCh <- ackMessage
		}(peer)
	}
	wg.Wait()
	close(responseCh)
}

// SendNewViewMessage sends a new view request to all peers and sends the accepted message to the response channel
func (p *Proposer) SendNewViewMessage(newViewMessage *pb.NewViewMessage, responseCh chan *pb.AcceptedMessage) {
	// Logger: Add sent new view message
	p.logger.AddSentNewViewMessage(newViewMessage)

	// Multicast new view request to all peers
	wg := sync.WaitGroup{}
	for _, peer := range p.peers {
		wg.Add(1)
		go func(peer *models.Node) {
			defer wg.Done()
			stream, err := peer.Client.NewViewRequest(context.Background(), newViewMessage)
			if err != nil {
				log.Warnf("[SendNewViewMessage] Failed to send new view message to peer %s: %v", peer.ID, err)
				return
			}

			for {
				acceptedMessage, err := stream.Recv()
				if err == io.EOF {
					log.Infof("[SendNewViewMessage] EOF received for peer %s", peer.ID)
					return
				}
				if err != nil {
					log.Warnf("[SendNewViewMessage] Error receiving accepted message from peer %s: %v", peer.ID, err)
					return
				}

				// Logger: Add received accepted message
				p.logger.AddReceivedAcceptedMessage(acceptedMessage)

				responseCh <- acceptedMessage
			}
		}(peer)
	}
	wg.Wait()
	close(responseCh)
}

// SendAcceptMessage sends an accept request to all peers and sends if accept is successful to the response channel
func (p *Proposer) SendAcceptMessage(acceptMessage *pb.AcceptMessage, responseCh chan bool) {
	// Logger: Add sent accept message
	p.logger.AddSentAcceptMessage(acceptMessage)

	// Multicast accept request to all peers
	wg := sync.WaitGroup{}
	log.Infof("[SendAcceptMessage] Sending accept message %s", utils.LoggingString(acceptMessage.Message))
	for _, peer := range p.peers {
		wg.Add(1)
		go func(peer *models.Node) {
			defer wg.Done()
			resp, err := peer.Client.AcceptRequest(context.Background(), acceptMessage)
			if err != nil {
				log.Warnf("[SendAcceptMessage] Failed to send accept message to peer %s: %v", peer.ID, err)
				responseCh <- false
				return
			}

			// Logger: Add received accepted message
			p.logger.AddReceivedAcceptedMessage(resp)

			responseCh <- true
		}(peer)
	}
	wg.Wait()
	close(responseCh)
}

// BroadcastCommitMessage sends a commit request to all peers
func (p *Proposer) BroadcastCommitMessage(commitMessage *pb.CommitMessage) {
	// Logger: Add sent commit message
	p.logger.AddSentCommitMessage(commitMessage)

	// Multicast commit request to all peers
	log.Infof("[BroadcastCommitMessage] Broadcasting commit request %s", utils.LoggingString(commitMessage.Message))
	for _, peer := range p.peers {
		go func(peer *models.Node) {
			_, err := peer.Client.CommitRequest(context.Background(), commitMessage)
			if err != nil {
				log.Warnf("[BroadcastCommitMessage] Failed to send commit message to peer %s: %v", peer.ID, err)
				return
			}
		}(peer)
	}
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
			_, err := peer.Client.CheckpointRequest(context.Background(), checkpointMessage)
			if err != nil {
				log.Warnf("[SendCheckpointMessage] Failed to send checkpoint message to peer %s: %v", peer.ID, err)
				return
			}
		}(peer)
	}
}
