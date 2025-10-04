package paxos

import (
	"context"
	"sync"

	paxospb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ProposerClient struct {
	NodeID string
	Peers  map[string]string
	Quorum int
}

func (p *ProposerClient) SendPrepareRequest(req *paxospb.PrepareMessage) (bool, error) {
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	acceptCount := 0
	for id, addr := range p.Peers {
		wg.Add(1)
		go func(id, addr string) {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn(err)
			}
			defer conn.Close()

			client := paxospb.NewPaxosClient(conn)
			resp, err := client.PrepareRequest(context.Background(), req)
			if err != nil {
				log.Warn(err)
			}
			if !resp.Ok {
				return
			}
			mu.Lock()
			acceptCount++
			mu.Unlock()
			wg.Done()
		}(id, addr)
	}

	wg.Wait()
	if acceptCount < p.Quorum {
		return false, nil
	}
	return true, nil

}

func (p *ProposerClient) SendAcceptRequest(req *paxospb.AcceptMessage) (bool, error) {

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}

	acceptCount := 0
	for id, addr := range p.Peers {
		wg.Add(1)
		go func(id, addr string) {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn(err)
			}
			defer conn.Close()

			client := paxospb.NewPaxosClient(conn)
			resp, err := client.AcceptRequest(context.Background(), req)
			if err != nil {
				log.Warn(err)
			}
			if !resp.Ok {
				return
			}
			mu.Lock()
			acceptCount++
			// TODO: need to log accept message (but why?)
			mu.Unlock()
			wg.Done()
		}(id, addr)
	}

	wg.Wait()
	if acceptCount < p.Quorum {
		return false, nil
	}
	return true, nil
}

func (p *ProposerClient) SendCommitRequest(req *paxospb.CommitMessage) error {
	for id, addr := range p.Peers {
		go func(id, addr string) {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn(err)
			}
			defer conn.Close()

			client := paxospb.NewPaxosClient(conn)
			_, err = client.CommitRequest(context.Background(), req)
			if err != nil {
				log.Warn(err)
			}
		}(id, addr)
	}
	return nil
}
