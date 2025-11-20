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
				log.Warn(err)
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
