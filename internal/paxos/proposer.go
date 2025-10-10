package paxos

import (
	"context"
	"sync"

	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *PaxosServer) SendPrepareRequest(req *pb.PrepareMessage) (bool, error) {

	// mu := sync.Mutex{}
	// acceptCount := 1
	peerAcceptLog := map[string][]*pb.AcceptRecord{}

	// Multicast prepare request to all peers except self
	// wg := sync.WaitGroup{}
	responseChan := make(chan bool, len(s.Peers)-1)
	for _, peer := range s.Peers {
		id := peer.ID
		if id == s.NodeID {
			continue
		}
		addr := peer.Address

		// wg.Add(1)
		log.Infof("Sending prepare request to %s", addr)
		go func(id, addr string) {
			// defer wg.Done()
			// Initialize connection to peer
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn(err)
			}
			defer conn.Close()
			client := pb.NewPaxosClient(conn)

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
				return // if not accepted, return without incrementing acceptCount
			}
			// mu.Lock()
			// defer mu.Unlock()
			// acceptCount++
			peerAcceptLog[id] = resp.AcceptLog
			responseChan <- true
		}(id, addr)
	}
	log.Infof("Waiting for prepare responses")
	// wg.Wait()
	// Count oks
	okCount := 1
	for i := 0; i < len(s.Peers)-1; i++ {
		ok := <-responseChan
		if ok {
			okCount++
		}
		if okCount >= s.Quorum {
			break
		}
	}

	// Check if acceptCount is less than quorum
	if okCount < s.Quorum {
		log.Warnf("I don't have enough acceptors to prepare %v", req.String())
		return false, nil
	}

	log.Infof("I am prepared %v", req.String())
	return true, nil

}

// SendAcceptRequest handles the accept request rpc on node client side
// This code is part of Proposer structure
func (s *PaxosServer) SendAcceptRequest(req *pb.AcceptMessage) (bool, error) {

	mu := sync.Mutex{}
	acceptCount := 1

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
			client := pb.NewPaxosClient(conn)

			// Send accept request to peer
			resp, err := client.AcceptRequest(context.Background(), req)
			if err != nil {
				log.Warn(err)
				return
			}
			if !resp.Ok {
				return // if not accepted, return without incrementing acceptCount
			}
			mu.Lock()
			defer mu.Unlock()
			acceptCount++
			// TODO: is server mutex needed here?; yes need to sync
			s.AcceptedMessages = append(s.AcceptedMessages, resp)
		}(id, addr)
	}
	wg.Wait()

	// Check if acceptCount is less than quorum
	if acceptCount < s.Quorum {
		return false, nil
	}
	return true, nil
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
			client := pb.NewPaxosClient(conn)
			_, err = client.CommitRequest(context.Background(), req)
			if err != nil {
				log.Warn(err)
			}
		}(id, addr)
	}
	return nil
}
