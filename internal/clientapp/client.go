package clientapp

import (
	"context"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// ClientRoutine is a persistent routine that processes transactions for a client
func ClientRoutine(ctx context.Context, clientID string, initialLeaderNode string, txnChan chan []*pb.Transaction, resetCh chan bool, signalCh chan bool, nodeMap map[string]*models.Node) {
	// Initialize leader node
	leaderNode := initialLeaderNode
	for {
		select {
		// Wait for set id to process from main routine
		case txn := <-txnChan:
			// Process transactions for the set
			for _, t := range txn {
				processTransaction(clientID, t, nodeMap, &leaderNode)
			}
			// Signal main routine that the set is done
			signalCh <- true

		// Reset signal
		case <-resetCh:
			// Reset leader node
			leaderNode = initialLeaderNode

		// Exit signal
		case <-ctx.Done():
			log.Infof("%s received exit signal", clientID)
			return
		}
	}
}
