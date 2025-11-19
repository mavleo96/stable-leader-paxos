package paxos

import (
	"context"
	"io"
	"sync"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// SendPrepareRequest sends a prepare request to all peers except self and returns the response from each peer to PrepareRoutine
// This code is part of Proposer structure
func (s *PaxosServer) SendPrepareRequest(req *pb.PrepareMessage) (bool, map[string][]*pb.AcceptRecord, error) {
	peerAcceptLog := map[string][]*pb.AcceptRecord{} // mutex not needed here since its a map

	// Multicast prepare request to all peers except self
	// We wait for only f+1 Goroutines to return
	responseChan := make(chan bool, len(s.Peers)-1)
	for _, peer := range s.Peers {
		id := peer.ID
		if id == s.NodeID {
			continue
		}
		addr := peer.Address

		log.Infof("Sending prepare request to %s", id)
		go func(id, addr string) {
			// Initialize connection to peer
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn(err)
			}
			defer conn.Close()
			client := pb.NewPaxosNodeClient(conn)

			// Send prepare request to peer
			resp, err := client.PrepareRequest(context.Background(), req)
			log.Infof("Prepare response from %s: %v", addr, resp.String())
			if err != nil {
				log.Warn(err)
				responseChan <- false
				return
			}
			if !resp.Ok {
				responseChan <- false
				return // if not accepted, return without incrementing promiseCount
			}
			peerAcceptLog[id] = resp.AcceptLog
			responseChan <- true
		}(id, addr)
	}
	log.Infof("Waiting for prepare responses")

	// Count promises and rejects
	promiseCount := int64(1) // 1 is for self
	rejectedCount := int64(0)
	for i := 0; i < len(s.Peers)-1; i++ {
		ok := <-responseChan
		if ok {
			promiseCount++
		} else {
			rejectedCount++
		}
		if rejectedCount >= s.config.F+1 || promiseCount >= s.config.F+1 {
			break
		}
	}

	// Check if promiseCount is less than quorum
	if promiseCount < s.config.F+1 {
		return false, nil, nil
	}
	return true, peerAcceptLog, nil
}

// SendNewViewRequest sends a new view request to all peers except self and returns the count of accepted requests for each sequence number
// This code is part of Proposer structure
func (s *PaxosServer) SendNewViewRequest(req *pb.NewViewMessage) (map[int64]int64, error) {
	// Initialize accept count map with 1 for self
	mu := sync.Mutex{}
	acceptCountMap := make(map[int64]int64)
	for _, record := range req.AcceptLog {
		acceptCountMap[record.AcceptedSequenceNum] = 1
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

// SendAcceptRequest handles the accept request rpc on node client side
// Returns: (accepted, rejected, error)
// This code is part of Proposer structure
func (s *PaxosServer) SendAcceptRequest(req *pb.AcceptMessage) (bool, bool, error) {

	mu := sync.Mutex{}
	acceptCount := int64(1)
	rejectedCount := int64(0)

	// Multicast accept request to all peers except self
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

			// Send accept request to peer
			resp, err := client.AcceptRequest(context.Background(), req)
			if err != nil {
				log.Warn(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if !resp.Ok {
				rejectedCount++
				return // if not accepted, return without incrementing acceptCount
			}
			acceptCount++
		}(id, addr)
	}
	wg.Wait()

	if rejectedCount >= s.config.F+1 {
		return false, true, nil
	}

	// Check if acceptCount is less than quorum
	if acceptCount >= s.config.F+1 {
		return true, false, nil
	}
	return false, false, nil
}

// SendCommitRequest handles the commit request rpc on node client side
// This code is part of Proposer structure
func (s *PaxosServer) SendCommitRequest(req *pb.CommitMessage) error {
	// Multicast commit request to all peers except self
	for _, peer := range s.Peers { // No need to wait for response from peers
		id := peer.ID
		if id == s.NodeID {
			continue // Skip self here
		}
		addr := peer.Address
		go func(id, addr string) {
			// Initialize connection to peer
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn(err)
			}
			defer conn.Close()

			// Send commit request to peer
			client := pb.NewPaxosNodeClient(conn)
			_, err = client.CommitRequest(context.Background(), req)
			if err != nil {
				log.Warn(err)
			}
		}(id, addr)
	}
	return nil
}

func (s *PaxosServer) SendCatchUpRequest(sequenceNum int64) (*pb.CatchupMessage, error) {
	// Multicast catch up request to all peers except self
	responseChan := make(chan *pb.CatchupMessage)
	for _, peer := range s.Peers {
		id := peer.ID
		if id == s.NodeID {
			continue
		}
		addr := peer.Address
		go func(id, addr string) {
			// Initialize connection to peer
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn(err)
			}
			defer conn.Close()
			client := pb.NewPaxosNodeClient(conn)
			record, err := client.CatchupRequest(context.Background(), &wrapperspb.Int64Value{Value: sequenceNum})
			if err != nil {
				log.Warn(err)
				responseChan <- nil
				return
			}
			responseChan <- record
		}(id, addr)
	}
	for i := 0; i < len(s.Peers)-1; i++ {
		catchupMessage := <-responseChan
		if catchupMessage != nil {
			return catchupMessage, nil
		}
	}
	return nil, status.Errorf(codes.Unavailable, "failed to get catch up message from leader")
}
