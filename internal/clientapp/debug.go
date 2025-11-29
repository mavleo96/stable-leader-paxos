package clientapp

import (
	"context"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// SendPrintLogCommand sends a print log command to all nodes
func SendPrintLogCommand(nodeMap map[string]*models.Node, testSet int64) error {
	log.Info("Print log command received")
	for _, node := range nodeMap {
		// Capture node in closure to avoid variable capture issues
		go func(n *models.Node) {
			_, err := (*n.Client).PrintLog(context.Background(), &wrapperspb.Int64Value{Value: testSet})
			if err != nil {
				log.Warnf("Error sending print log command to node %s: %v", n.ID, err)
			}
		}(node)
	}
	return nil
}

// SendPrintDBCommand sends a print db command to all nodes
func SendPrintDBCommand(nodeMap map[string]*models.Node, testSet int64) error {
	log.Info("Print db command received")
	for _, node := range nodeMap {
		// Capture node in closure to avoid variable capture issues
		go func(n *models.Node) {
			_, err := (*n.Client).PrintDB(context.Background(), &wrapperspb.Int64Value{Value: testSet})
			if err != nil {
				log.Warnf("Error sending print db command to node %s: %v", n.ID, err)
			}
		}(node)
	}
	return nil
}

// SendPrintStatusCommand sends a print status command to all nodes
func SendPrintStatusCommand(nodeMap map[string]*models.Node, testSet int64, sequenceNum int64) error {
	if sequenceNum == 0 {
		log.Infof("Print status command received for all sequence numbers")
	} else {
		log.Infof("Print status command received for %d", sequenceNum)
	}
	for _, node := range nodeMap {
		// Capture node in closure to avoid variable capture issues
		go func(n *models.Node) {
			_, err := (*n.Client).PrintStatus(context.Background(), &pb.StatusRequest{TestSet: testSet, SequenceNum: sequenceNum})
			if err != nil {
				log.Warnf("Error sending print status command to node %s: %v", n.ID, err)
			}
		}(node)
	}
	return nil
}

// SendPrintViewCommand sends a print view command to all nodes
func SendPrintViewCommand(nodeMap map[string]*models.Node, testSet int64) error {
	log.Info("Print view command received")
	for _, node := range nodeMap {
		// Capture node in closure to avoid variable capture issues
		go func(n *models.Node) {
			_, err := (*n.Client).PrintView(context.Background(), &wrapperspb.Int64Value{Value: testSet})
			if err != nil {
				log.Warnf("Error sending print view command to node %s: %v", n.ID, err)
			}
		}(node)
	}
	return nil
}
