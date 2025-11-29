package clientapp

import (
	"github.com/mavleo96/stable-leader-paxos/internal/models"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

// TestSet represents a test set with a set number, transactions, live nodes, byzantine nodes, and attacks
type TestSet struct {
	SetNumber         int64
	TransactionsLists []TransactionListMap
	Overrides         []Override
	Live              []*models.Node
}

// TransactionListMap represents a map of client IDs to transaction lists
type TransactionListMap map[string][]*pb.Transaction

// Override represents an override for a node
type Override struct {
	OverrideType string
}
