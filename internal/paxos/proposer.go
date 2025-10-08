package paxos

import (
	"context"
	"sync"

	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// func (s *PaxosServer) SendPrepareRequest(req *pb.PrepareMessage) (bool, error) {
// 	mu := sync.Mutex{}
// 	wg := sync.WaitGroup{}

// 	acceptCount := 0
// 	// for id, addr := range p.Peers {
// 	log.Infof("Sending prepare request")
// 	for _, peer := range s.Peers {
// 		id := peer.ID
// 		addr := peer.Address
// 		wg.Add(1)
// 		go func(id, addr string) {
// 			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 			if err != nil {
// 				log.Warn(err)
// 			}
// 			defer conn.Close()

// 			client := pb.NewPaxosClient(conn)
// 			resp, err := client.PrepareRequest(context.Background(), req)
// 			if err != nil {
// 				log.Warn(err)
// 			}
// 			if !resp.Ok {
// 				return
// 			}
// 			mu.Lock()
// 			acceptCount++
// 			mu.Unlock() // defer is better?
// 			wg.Done()   // defer is better?
// 		}(id, addr)
// 	}

// 	wg.Wait()
// 	if acceptCount < s.Quorum {
// 		return false, nil
// 	}
// 	s.State.Mutex.Lock()
// 	defer s.State.Mutex.Unlock()
// 	s.State.PromisedBallotNum = req.B
// 	// s.CurrentBallotNum = req.B
// 	// TODO: need to regularize the logs and prepare correct accept message

// 	log.Infof("Prepared %v", req.String())

// 	return true, nil

// }

// SendAcceptRequest handles the accept request rpc on node client side
// This code is part of Proposer structure
func (s *PaxosServer) SendAcceptRequest(req *pb.AcceptMessage) (bool, error) {

	mu := sync.Mutex{}
	acceptCount := 1

	// Multicast accept request to all peers
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
func (s *PaxosServer) SendCommitRequest(req *pb.CommitMessage) {
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
}
