package client

import (
	"context"
	"time"

	bankpb "github.com/mavleo96/cft-mavleo96/pb/bank"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"
)

const (
	clientTimeout = time.Second * 5
	maxAttempts   = 1000
)

// result is a struct to store the result of a transaction
type result struct {
	NodeID   string
	Response *bankpb.TransactionResponse
	Err      error
}

// ClientRoutine is a persistent routine that processes transactions for a client
func ClientRoutine(clientID string, signalCh chan SetNumber, txnQueue ClientTxnQueue, nodeClients map[string]bankpb.TransactionServiceClient) {
	leaderNode := "n1" // leader initialized to n1 by default
	for {
		// Wait for set id to process from main routine
		setNum := <-signalCh
		if setNum.N1 == -1 { // exit signal
			return
		}

		// Process transactions for the set
		for _, t := range txnQueue[setNum] {
			processTransaction(clientID, t, nodeClients, &leaderNode)
		}
		// Signal main routine that the set is done
		signalCh <- SetNumber{}
	}
}

// processTransaction processes a transaction with retries and updates the leader node if leader changes
func processTransaction(clientID string, t *bankpb.Transaction, nodeClients map[string]bankpb.TransactionServiceClient, leaderNode *string) {
	// Create a TransactionRequest with timestamp (uid) and sender
	timestamp := time.Now().UnixMilli()
	request := &bankpb.TransactionRequest{
		Transaction: t,
		Timestamp:   timestamp,
		Sender:      clientID,
	}

	// Retry loop
	var err error
	var response *bankpb.TransactionResponse
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Context for each attempt with timeout
		ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)

		if attempt == 1 { // First attempt to leader
			response, err = nodeClients[*leaderNode].TransferRequest(ctx, request)
		} else { // Multi-cast to all nodes if 1st attempt fails
			// Responses channel to collect responses from goroutines of requests to all nodes
			// (By recreating this channel in each attempt we don't have to drain it)
			responsesCh := make(chan result, len(nodeClients))
			// TODO: need a context manager for multi-casted request

			// Multi-cast to all nodes and collect responses
			for nodeID, nodeClient := range nodeClients {
				go func(nodeID string, nodeClient bankpb.TransactionServiceClient) {
					resp, err := nodeClient.TransferRequest(ctx, request)
					responsesCh <- result{NodeID: nodeID, Response: resp, Err: err}
				}(nodeID, nodeClient)
			}
			for i := 0; i < len(nodeClients); i++ { // Collect responses from goroutines
				res := <-responsesCh
				if res.Err == nil {
					// Update leader node if response is successful
					*leaderNode = res.NodeID
					response = res.Response
					err = nil
					log.Infof("%s updated leader to %s", clientID, *leaderNode)
					break
				} else {
					err = res.Err
				}
			}
		}
		// Cancel context and break from retry loop if error is nil
		cancel()
		if err == nil {
			log.Infof(
				"%s <- %s: {Request: ((%s, %s, %d), %d), Success: %v}",
				clientID,
				*leaderNode,
				request.Transaction.Sender,
				request.Transaction.Receiver,
				request.Transaction.Amount,
				request.Timestamp,
				response.Success,
			)
			break
		} else {
			log.Warnf(
				"%s <- %s: {Request: ((%s, %s, %d), %d), Error: %v}",
				clientID,
				*leaderNode,
				request.Transaction.Sender,
				request.Transaction.Receiver,
				request.Transaction.Amount,
				request.Timestamp,
				status.Convert(err).Message(),
			)
		}
	}

}
