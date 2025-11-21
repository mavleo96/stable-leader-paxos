package paxos

import (
	"context"
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SendCatchUpRequest sends a catch up request to the leader
func (s *PaxosServer) SendCatchUpRequest(sequenceNum int64) (*pb.CatchupMessage, error) {

	catchupRequest := &pb.CatchupRequestMessage{NodeID: s.ID, SequenceNum: sequenceNum}

	// Multicast catch up request to all peers except self
	responseChan := make(chan *pb.CatchupMessage)
	wg := sync.WaitGroup{}
	for _, peer := range s.peers {
		wg.Add(1)
		go func(peer *models.Node) {
			defer wg.Done()
			catchupMessage, err := (*peer.Client).CatchupRequest(context.Background(), catchupRequest)
			if err != nil || catchupMessage == nil {
				log.Warnf("[SendCatchUpRequest] Failed to get catch up message from leader %s: %v", peer.ID, err)
				return
			}
			responseChan <- catchupMessage
		}(peer)
	}
	go func() {
		wg.Wait()
		close(responseChan)
	}()

	catchupMessage, ok := <-responseChan
	if !ok {
		return nil, status.Errorf(codes.Unavailable, "failed to get catch up message from leader")
	}
	return catchupMessage, nil
}

// CatchupRoutine is the main routine for the catch up
func (s *PaxosServer) CatchupRoutine() {
	maxSequenceNum := s.state.StateLog.MaxSequenceNum()
	catchupMessage, err := s.SendCatchUpRequest(maxSequenceNum)
	if err != nil {
		log.Warn(err)
		return
	}

	log.Infof("[CatchupRoutine] Received catch up message from leader %s: %v", catchupMessage.B.NodeID, catchupMessage.B)
	s.state.SetLeader(catchupMessage.B.NodeID)
	s.state.SetBallotNumber(catchupMessage.B)
	for _, record := range catchupMessage.CommitLog {
		s.acceptor.CommitRequestHandler(record)
	}
}
