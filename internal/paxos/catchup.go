package paxos

import (
	"context"
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// InitiateCatchupHandler is the handler for the catch up
func (s *PaxosServer) InitiateCatchupHandler() {
	maxSequenceNum := s.state.GetLastExecutedSequenceNum()
	catchupMessage, err := s.SendCatchUpRequest(maxSequenceNum)
	log.Infof("[InitiateCatchupHandler] Got catch up message from leader: %s", catchupMessage.String())
	if err != nil {
		log.Warnf("[InitiateCatchupHandler] Failed to send catch up request: %v", err)
		return
	}
	if !s.phaseManager.HandleBallotNumber(catchupMessage.B) {
		log.Warnf("[InitiateCatchupHandler] Failed to check and update phase for ballot number %s", utils.LoggingString(catchupMessage.B))
		return
	}
	log.Infof("[InitiateCatchupHandler] Received catch up message from leader %s: %v, msg: %s", catchupMessage.B.NodeID, catchupMessage.B, catchupMessage.String())

	// Install checkpoint if not nil
	checkpoint := catchupMessage.Checkpoint
	if checkpoint != nil {
		log.Infof("[InitiateCatchupHandler] Installing checkpoint for sequence number %d, %s", checkpoint.SequenceNum, checkpoint.String())
		s.executor.checkpointer.AddCheckpoint(checkpoint.SequenceNum, checkpoint.Snapshot, checkpoint.DedupTableTimestamp, checkpoint.DedupTableResult)

		signalCh := make(chan struct{}, 1)
		checkpointInstallRequest := CheckpointInstallRequest{
			SequenceNum: checkpoint.SequenceNum,
			SignalCh:    signalCh,
		}
		s.executor.GetInstallCheckpointChannel() <- checkpointInstallRequest
		<-signalCh
		s.executor.checkpointer.Purge(checkpoint.SequenceNum)
	}

	// Handle commit log
	for _, record := range catchupMessage.CommitLog {
		go s.acceptor.CommitRequestHandler(record)
	}
	log.Infof("[InitiateCatchupHandler] Finished catching up")
}

// SendCatchUpRequest sends a catch up request to the leader
func (s *PaxosServer) SendCatchUpRequest(sequenceNum int64) (*pb.CatchupMessage, error) {
	catchupRequest := &pb.CatchupRequestMessage{NodeID: s.ID, SequenceNum: sequenceNum}

	// Logger: Add sent catchup request message
	s.logger.AddSentCatchupRequestMessage(catchupRequest)

	// Multicast catch up request to all peers except self
	responseChan := make(chan *pb.CatchupMessage, len(s.peers))
	wg := sync.WaitGroup{}
	for _, peer := range s.peers {
		wg.Add(1)
		go func(peer *models.Node) {
			defer wg.Done()
			catchupMessage, err := peer.Client.CatchupRequest(context.Background(), catchupRequest)
			if err != nil || catchupMessage == nil {
				return
			}
			responseChan <- catchupMessage
		}(peer)
	}
	go func() {
		wg.Wait()
		close(responseChan)
	}()

	// Wait for response and return the catch up message
	catchupMessage, ok := <-responseChan
	if !ok {
		log.Warnf("[SendCatchUpRequest] Failed to get catch up message from leader")
		return nil, status.Errorf(codes.Unavailable, "failed to get catch up message from leader")
	}
	log.Infof("[SendCatchUpRequest] Received catch up message from leader: %s", catchupMessage.String())

	// Logger: Add received catchup message
	s.logger.AddReceivedCatchupMessage(catchupMessage)

	return catchupMessage, nil
}
