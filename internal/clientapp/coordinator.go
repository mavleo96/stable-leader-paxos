package clientapp

import (
	"context"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

// Coordinator is the main routine that coordinates the test sets and clients
func Coordinator(ctx context.Context, nodeMap map[string]*models.Node, testSetCh chan TestSet, clientChannels map[string]chan []*pb.Transaction, signalCh chan bool) {
	// Initialize test set queue, current test set index, and current transaction list index
	testSetQueue := make([]TestSet, 0)
	currentTestSetIndex := 0
	currentTransactionListIndex := 0

	// Initialize active client count
	activeClientCount := 0

	// Assign loop
assignLoop:
	for {
		select {
		case <-ctx.Done():
			log.Info("Main: Coordinator context done")
			return

		// Test set received from main routine
		case testSet := <-testSetCh:
			testSetQueue = append(testSetQueue, testSet)

			// Reconfigure nodes to live nodes
			ReconfigureNodes(nodeMap, testSet.Live)

			// Send transactions to clients if no clients are active
			if activeClientCount == 0 {
				// Bounds check before accessing queue
				if currentTestSetIndex >= len(testSetQueue) {
					log.Warnf("Coordinator: currentTestSetIndex %d out of bounds for queue length %d", currentTestSetIndex, len(testSetQueue))
					continue assignLoop
				}
				currentTestSet := testSetQueue[currentTestSetIndex]
				// Bounds check before accessing transaction lists
				if currentTransactionListIndex >= len(currentTestSet.TransactionsLists) {
					log.Warnf("Coordinator: currentTransactionListIndex %d out of bounds for test set %d", currentTransactionListIndex, currentTestSet.SetNumber)
					continue assignLoop
				}
				currentTxnList := currentTestSet.TransactionsLists[currentTransactionListIndex]
				for clientID, txnCh := range clientChannels {
					txnCh <- currentTxnList[clientID]
					activeClientCount++
				}
			}

		// Signal received from client routine
		case <-signalCh:
			activeClientCount--

			// If all clients are done, increment transaction list index & send next transaction list
			if activeClientCount == 0 {
				// Bounds check before accessing queue
				if currentTestSetIndex >= len(testSetQueue) {
					log.Warnf("Coordinator: currentTestSetIndex %d out of bounds for queue length %d", currentTestSetIndex, len(testSetQueue))
					continue assignLoop
				}
				currentTestSet := testSetQueue[currentTestSetIndex]
				log.Infof("Coordinator: All clients are done for test set %d part %d", currentTestSet.SetNumber, currentTransactionListIndex+1)

				// If there are more transaction lists, perform override operation
				if currentTransactionListIndex+1 < len(currentTestSet.TransactionsLists) {
					// Bounds check before accessing overrides
					if currentTransactionListIndex < len(currentTestSet.Overrides) {
						overrideType := currentTestSet.Overrides[currentTransactionListIndex].OverrideType
						switch overrideType {
						case "kill leader":
							KillLeader(nodeMap)
						default:
							log.Warnf("Coordinator: Unknown override type: %s", overrideType)
						}
					}

					// Increment transaction list index and send next transaction list
					currentTransactionListIndex++
					// Bounds check before accessing transaction lists
					if currentTransactionListIndex >= len(currentTestSet.TransactionsLists) {
						log.Warnf("Coordinator: currentTransactionListIndex %d out of bounds for test set %d", currentTransactionListIndex, currentTestSet.SetNumber)
						continue assignLoop
					}
					currentTxnList := currentTestSet.TransactionsLists[currentTransactionListIndex]
					for clientID, txnCh := range clientChannels {
						txnCh <- currentTxnList[clientID]
						activeClientCount++
					}
					continue assignLoop
				}

				// If all transaction lists are completed, increment test set index and send next test set if there are more test sets
				if currentTransactionListIndex+1 == len(currentTestSet.TransactionsLists) {
					log.Infof("Coordinator: Test set %d completed", currentTestSet.SetNumber)
					currentTestSetIndex++
					currentTransactionListIndex = 0

					// Send transactions to clients if active count is 0
					if currentTestSetIndex < len(testSetQueue) {
						currentTestSet = testSetQueue[currentTestSetIndex]
						// Bounds check before accessing transaction lists
						if currentTransactionListIndex >= len(currentTestSet.TransactionsLists) {
							log.Warnf("Coordinator: currentTransactionListIndex %d out of bounds for test set %d", currentTransactionListIndex, currentTestSet.SetNumber)
							continue assignLoop
						}
						currentTxnList := currentTestSet.TransactionsLists[currentTransactionListIndex]
						for clientID, txnCh := range clientChannels {
							txnCh <- currentTxnList[clientID]
							activeClientCount++
						}
					}
				}
			}
		}
	}
}
