package clientapp

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"

	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

// SetNumber represent test set number
// N1 is the set number
// N2 is the subset number (set divided into subsets by LF operation)
type SetNumber struct {
	N1 int64
	N2 int64
}

// Custom types for transaction queues
type ClientTxnQueue map[SetNumber][]*pb.Transaction
type MasterTxnQueue map[string]ClientTxnQueue

// ReadCSV reads records from a csv file at given path
func ReadCSV(path string) ([][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return [][]string{}, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return [][]string{}, err
	}
	return records, nil
}

// ParseRecords parses the records and creates a master queue of transactions
// Returns a map of client id to set number to list of transaction pointers, a list of set numbers, a map of set number to list of alive nodes, and an error
func ParseRecords(records [][]string, clientList []string) (MasterTxnQueue, []SetNumber, map[SetNumber][]string, error) {
	// txnQueue is a nested map of client id to set number to list of transaction pointers
	masterQueue := make(MasterTxnQueue)
	for _, clientID := range clientList {
		masterQueue[clientID] = make(ClientTxnQueue)
	}

	// aliveNodesMap is a map of set number to list of alive nodes
	aliveNodesMap := make(map[SetNumber][]string)

	// Process records
	var setNumList []SetNumber
	for i, record := range records {
		if i == 0 {
			continue // skip header row
		}

		// If set number is new, initialize new lists for each client, add set number to set number list
		// and add alive nodes to alive nodes map
		if record[0] != "" {
			// Parse set number
			n1, err := strconv.ParseInt(record[0], 10, 64)
			if err != nil {
				return nil, nil, nil, err
			}
			setNum := SetNumber{N1: n1, N2: 1}

			// Parse alive nodes
			aliveNodes := parseNodeString(record[2])
			aliveNodesMap[setNum] = aliveNodes
			setNumList = append(setNumList, setNum)
			for _, clientID := range clientList {
				masterQueue[clientID][*utils.LastElement(setNumList)] = make([]*pb.Transaction, 0)
			}

		}

		// If record is LF, add new set number to set number list
		if record[1] == "LF" {
			setNumList = append(setNumList, SetNumber{
				N1: (*utils.LastElement(setNumList)).N1,
				N2: (*utils.LastElement(setNumList)).N2 + 1,
			})

			// Initialize new lists for each client
			for _, clientID := range clientList {
				masterQueue[clientID][*utils.LastElement(setNumList)] = make([]*pb.Transaction, 0)
			}
			continue // LF is not a transaction
		}

		// Parse and append transaction
		t, err := parseTransactionString(record[1])
		if err != nil {
			return nil, nil, nil, err
		}
		masterQueue[t.Sender][*utils.LastElement(setNumList)] = append(masterQueue[t.Sender][*utils.LastElement(setNumList)], &t)
	}
	return masterQueue, setNumList, aliveNodesMap, nil
}

// parseTransactionString parses a transaction string of the format "(Sender, Receiver, Amount)"
func parseTransactionString(s string) (pb.Transaction, error) {
	var p []string = strings.Split(strings.Trim(s, "()\""), ", ")

	amount, err := strconv.Atoi(p[2])
	if err != nil {
		return pb.Transaction{}, err
	}

	return pb.Transaction{
		Sender:   p[0],
		Receiver: p[1],
		Amount:   int64(amount)}, nil
}

// parseNodeString parses a string representation of a list of nodes of the format "[n1, n2, n3]"
func parseNodeString(s string) []string {
	return strings.Split(strings.Trim(s, "[]\""), ", ")
}
