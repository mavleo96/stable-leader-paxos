package paxos

import (
	"context"
	"io"
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// // SendPrepareRequest sends a prepare request to all peers except self and returns the response from each peer to PrepareRoutine
// // This code is part of Proposer structure
// func (s *PaxosServer) SendPrepareRequest(req *pb.PrepareMessage) (bool, map[string][]*pb.AcceptRecord, error) {
// 	peerAcceptLog := map[string][]*pb.AcceptRecord{} // mutex not needed here since its a map

// 	// Multicast prepare request to all peers except self
// 	// We wait for only f+1 Goroutines to return
// 	responseChan := make(chan bool, len(s.Peers)-1)
// 	for _, peer := range s.Peers {
// 		id := peer.ID
// 		if id == s.NodeID {
// 			continue
// 		}
// 		addr := peer.Address

// 		log.Infof("Sending prepare request to %s", id)
// 		go func(id, addr string) {
// 			// Initialize connection to peer
// 			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 			if err != nil {
// 				log.Warn(err)
// 			}
// 			defer conn.Close()
// 			client := pb.NewPaxosNodeClient(conn)

// 			// Send prepare request to peer
// 			resp, err := client.PrepareRequest(context.Background(), req)
// 			log.Infof("Prepare response from %s: %v", addr, resp.String())
// 			if err != nil {
// 				log.Warn(err)
// 				responseChan <- false
// 				return
// 			}
// 			if !resp.Ok {
// 				responseChan <- false
// 				return // if not accepted, return without incrementing promiseCount
// 			}
// 			peerAcceptLog[id] = resp.AcceptLog
// 			responseChan <- true
// 		}(id, addr)
// 	}
// 	log.Infof("Waiting for prepare responses")

// 	// Count promises and rejects
// 	promiseCount := int64(1) // 1 is for self
// 	rejectedCount := int64(0)
// 	for i := 0; i < len(s.Peers)-1; i++ {
// 		ok := <-responseChan
// 		if ok {
// 			promiseCount++
// 		} else {
// 			rejectedCount++
// 		}
// 		if rejectedCount >= s.config.F+1 || promiseCount >= s.config.F+1 {
// 			break
// 		}
// 	}

// 	// Check if promiseCount is less than quorum
// 	if promiseCount < s.config.F+1 {
// 		return false, nil, nil
// 	}
// 	return true, peerAcceptLog, nil
// }

// SendNewViewRequest sends a new view request to all peers except self and returns the count of accepted requests for each sequence number
// This code is part of Proposer structure
func (s *PaxosServer) SendNewViewRequest(req *pb.NewViewMessage) (map[int64]int64, error) {
	// Initialize accept count map with 1 for self
	mu := sync.Mutex{}
	acceptCountMap := make(map[int64]int64)
	for _, record := range req.AcceptLog {
		acceptCountMap[record.SequenceNum] = 1
	}

	// Multicast new view request to all peers except self
	wg := sync.WaitGroup{}
	for _, peer := range s.Peers {
		id := peer.ID
		if id == s.NodeID {
			continue
		}
		addr := peer.Address
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			// Initialize connection to peer
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn(err)
			}
			defer conn.Close()
			client := pb.NewPaxosNodeClient(conn)

			// Send new view request to peer
			stream, err := client.NewViewRequest(context.Background(), req)
			if err != nil {
				log.Warn(err)
				return
			}
			// Stream accept responses from peer
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Warn(err)
					return
				}
				// Update accept count map
				mu.Lock()
				acceptCountMap[resp.SequenceNum]++
				mu.Unlock()
			}
		}(id, addr)
	}
	wg.Wait()

	return acceptCountMap, nil
}

func (s *PaxosServer) SendCatchUpRequest(sequenceNum int64) (*pb.CatchupMessage, error) {

	catchupRequest := &pb.CatchupRequestMessage{NodeID: s.NodeID, SequenceNum: sequenceNum}

	// Multicast catch up request to all peers except self
	responseChan := make(chan *pb.CatchupMessage)
	wg := sync.WaitGroup{}
	for _, peer := range s.Peers {
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
