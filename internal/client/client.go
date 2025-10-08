package client

import (
	"context"
	"time"

	"github.com/mavleo96/cft-mavleo96/internal/utils"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"
)

const (
	clientTimeout = 1500 * time.Millisecond
	maxAttempts   = 1000
)

// result is a struct to store the result of a transaction
type result struct {
	Response *pb.TransactionResponse
	Err      error
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
					"%s <- %s: %s, %s hola",
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
					"%s <- %s: %s, %v amigo",
					clientID,
					*leaderNode,
					utils.TransactionString(request.Transaction),
					status.Convert(err).Message(),
				)
				cancel()
				continue retryLoop
			}
		} else { // Multi-cast to all nodes if 1st attempt fails
			// Responses channel to collect responses from goroutines of requests to all nodes
			// (By recreating this channel in each attempt we don't have to drain it)
			responsesCh := make(chan result, len(nodeClients))
			// TODO: need a context manager for multi-casted request

			// Multi-cast to all nodes and collect responses
			for nodeID, nodeClient := range nodeClients {
				go func(nodeID string, nodeClient pb.PaxosClient) {
					resp, err := nodeClient.TransferRequest(ctx, request)
					if err == nil {
						log.Infof(
							"%s <- %s: %s, %s buenos",
							clientID,
							nodeID,
							utils.TransactionString(request.Transaction),
							utils.TransactionResponseString(resp),
						)
					} else {
						log.Warnf(
							"%s <- %s: %s, %v dias",
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
			cancel()
			continue retryLoop
		}
	}

}
