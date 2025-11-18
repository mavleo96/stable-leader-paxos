package client

import (
	"context"
	"sync"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"
)

const (
	clientTimeout = 500 * time.Millisecond
	maxAttempts   = 1000
)

// result is a struct to store the result of a transaction
type result struct {
	Response *pb.TransactionResponse
	Err      error
}

func QueueRoutine(ctx context.Context, queueChan chan SetNumber, clientChannels map[string]chan SetNumber, nodeClients map[string]pb.PaxosClient) {
	for {
		select {
		case setNum := <-queueChan:
			if setNum.N2 > 1 {
				KillLeader(nodeClients)
			}
			wg := sync.WaitGroup{}
			for _, clientChan := range clientChannels {
				wg.Add(1)
				go func(clientChan chan SetNumber) {
					defer wg.Done()
					clientChan <- setNum
					<-clientChan
				}(clientChan)
			}
			wg.Wait()
			log.Infof("Set (%d, %d) processed", setNum.N1, setNum.N2)
		case <-ctx.Done():
			log.Info("Queue routine received exit signal")
			return
		}
	}
}

// ClientRoutine is a persistent routine that processes transactions for a client
func ClientRoutine(ctx context.Context, clientID string, signalCh chan SetNumber, txnQueue ClientTxnQueue, nodeClients map[string]pb.PaxosClient) {
	leaderNode := "n1" // leader initialized to n1 by default
	for {
		select {
		// Wait for set id to process from main routine
		case setNum := <-signalCh:
			// Process transactions for the set
			for _, t := range txnQueue[setNum] {
				processTransaction(clientID, t, nodeClients, &leaderNode)
			}
			// Signal main routine that the set is done
			signalCh <- SetNumber{}

		// Exit signal
		case <-ctx.Done():
			log.Infof("%s received exit signal", clientID)
			return
		}
	}
}

// processTransaction processes a transaction with retries and updates the leader node if leader changes
func processTransaction(clientID string, t *pb.Transaction, nodeClients map[string]pb.PaxosClient, leaderNode *string) {
	// Create a TransactionRequest with timestamp (uid) and sender
	timestamp := time.Now().UnixMilli()
	request := &pb.TransactionRequest{
		Transaction: t,
		Timestamp:   timestamp,
		Sender:      clientID,
	}

	// Retry loop
	var err error
	var response *pb.TransactionResponse
retryLoop:
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Context for each attempt with timeout
		ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
		if attempt == 1 { // First attempt to leader
			response, err = nodeClients[*leaderNode].TransferRequest(ctx, request)
			if err == nil {
				log.Infof(
					"%s <- %s: %s, %s",
					clientID,
					*leaderNode,
					utils.TransactionString(request.Transaction),
					utils.TransactionResponseString(response),
				)
				cancel()
				break retryLoop
				// return
			} else {
				log.Warnf(
					"%s <- %s: %s, %v",
					clientID,
					*leaderNode,
					utils.TransactionString(request.Transaction),
					status.Convert(err).Message(),
				)
				cancel()
				time.Sleep(clientTimeout)
				continue retryLoop
			}
		} else { // Multi-cast to all nodes if 1st attempt fails
			// Responses channel to collect responses from goroutines of requests to all nodes
			// (By recreating this channel in each attempt we don't have to drain it)
			responsesCh := make(chan result, len(nodeClients))
			// Multi-cast to all nodes and collect responses
			for nodeID, nodeClient := range nodeClients {
				go func(nodeID string, nodeClient pb.PaxosClient) {
					resp, err := nodeClient.TransferRequest(ctx, request)
					if err == nil {
						log.Infof(
							"%s <- %s: %s, %s",
							clientID,
							nodeID,
							utils.TransactionString(request.Transaction),
							utils.TransactionResponseString(resp),
						)
					} else {
						log.Warnf(
							"%s <- %s: %s, %v",
							clientID,
							nodeID,
							utils.TransactionString(request.Transaction),
							status.Convert(err).Message(),
						)
					}
					responsesCh <- result{Response: resp, Err: err}
				}(nodeID, nodeClient)
			}
			for i := 0; i < len(nodeClients); i++ { // Collect responses from goroutines
				res := <-responsesCh
				if res.Err == nil {
					// Update leader node if response is successful
					*leaderNode = res.Response.B.NodeID
					err = nil
					log.Infof("%s updated leader to %s", clientID, *leaderNode)
					cancel()
					break retryLoop
				}
			}
			time.Sleep(clientTimeout)
			cancel()
			continue retryLoop
		}
	}

}
