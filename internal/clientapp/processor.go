package clientapp

import (
	"context"
	"sync"
	"time"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/status"
)

const (
	clientTimeout = 500 * time.Millisecond
	maxAttempts   = 1000
)

// processTransaction processes a transaction with retries and updates the leader node if leader changes
func processTransaction(clientID string, t *pb.Transaction, nodeMap map[string]*models.Node, leaderNode *string) {
	// Create a TransactionRequest with timestamp (uid) and sender
	timestamp := time.Now().UnixMilli()
	request := &pb.TransactionRequest{
		Transaction: t,
		Timestamp:   timestamp,
		Sender:      clientID,
	}

	// Retry loop
retryLoop:
	for attempt := 1; attempt <= maxAttempts; attempt++ {

		// Send request to leader node if 1st attempt, otherwise send request to all nodes
		// and collect responses in a channel
		responseCh := make(chan *pb.TransactionResponse, len(nodeMap))
		wg := sync.WaitGroup{}
		if attempt == 1 { // First attempt to leader
			wg.Add(1)
			go func(node *models.Node) {
				defer wg.Done()
				response, err := (*node.Client).TransferRequest(context.Background(), request)
				if err == nil {
					log.Infof("%s <- %s: %s, %s", clientID, *leaderNode, utils.LoggingString(request), utils.LoggingString(response))
				} else {
					log.Warnf("%s <- %s: %s, %v", clientID, *leaderNode, utils.LoggingString(request), status.Convert(err).Message())
					return
				}
				responseCh <- response
			}(nodeMap[*leaderNode])
		} else {
			// Multi-cast to all nodes if 1st attempt fails and collect responses
			for _, node := range nodeMap {
				wg.Add(1)
				go func(node *models.Node) {
					defer wg.Done()
					response, err := (*node.Client).TransferRequest(context.Background(), request)
					if err == nil {
						log.Infof("%s <- %s: %s, %s", clientID, node.ID, utils.LoggingString(request), utils.LoggingString(response))
					} else {
						log.Warnf("%s <- %s: %s, %v", clientID, node.ID, utils.LoggingString(request), status.Convert(err).Message())
						return
					}
					responseCh <- response
				}(node)
			}
		}

		// Wait for response or timeout
		select {
		case <-time.After(clientTimeout):
			// Timeout: cleanup in background and retry
			go func() {
				wg.Wait()
				close(responseCh)
			}()
			continue retryLoop
		case response := <-responseCh:
			go func() {
				wg.Wait()
				close(responseCh)
			}()

			// Update leader node if response is not nil and leader node has changed
			if response != nil && *leaderNode != response.B.NodeID {
				*leaderNode = response.B.NodeID
				log.Infof("%s updated leader to %s", clientID, *leaderNode)
			}
			return
		}
	}
}
