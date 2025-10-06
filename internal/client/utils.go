package client

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"

	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
)

// SetNumber represent test set number
// N1 is the set number
// N2 is the subset number (set divided into subsets by LF operation)
type SetNumber struct {
	N1 int
	N2 int
}

// Custom types for transaction queues
type ClientTxnQueue map[SetNumber][]*pb.Transaction
type MasterTxnQueue map[string]ClientTxnQueue

// CreateMasterQueue creates a master queue of transactions
// Returns a map of client id to set number to list of transaction pointers
func CreateMasterQueue(records [][]string, clientList []string) (MasterTxnQueue, []SetNumber, error) {
	// txnQueue is a nested map of client id to set number to list of transaction pointers
	masterQueue := make(MasterTxnQueue)
	for _, clientID := range clientList {
		masterQueue[clientID] = make(ClientTxnQueue)
	}

	// Process records
	var setNumList []SetNumber
	for i, record := range records {
		if i == 0 {
			continue // skip header row
		}

		// If set number is new, initialize new lists for each client
		if record[0] != "" {
			n1, err := strconv.Atoi(record[0])
			if err != nil {
				return nil, nil, err
			}
			setNumList = append(setNumList, SetNumber{N1: n1, N2: 1})
			for _, clientID := range clientList {
				masterQueue[clientID][lastElement(setNumList)] = make([]*pb.Transaction, 0)
			}
			// TODO: need to save alive nodes list here
		}
		if record[1] == "LF" {
			setNumList = append(setNumList, SetNumber{
				N1: lastElement(setNumList).N1,
				N2: lastElement(setNumList).N2 + 1,
			})
			for _, clientID := range clientList {
				masterQueue[clientID][lastElement(setNumList)] = make([]*pb.Transaction, 0)
			}
			continue // LF is not a transaction
		}
		// Parse and append transaction
		t, err := ParseTransactionString(record[1])
		if err != nil {
			return nil, nil, err
		}
		masterQueue[t.Sender][lastElement(setNumList)] = append(masterQueue[t.Sender][lastElement(setNumList)], &t)
	}
	return masterQueue, setNumList, nil
}

// ParseTransactionString parses a transaction string of the format "(Sender, Receiver, Amount)"
func ParseTransactionString(s string) (pb.Transaction, error) {
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

// ParseNodeString parses a string representation of a list of nodes of the format "[n1, n2, n3]"
func ParseNodeString(s string) []string {
	return strings.Split(strings.Trim(s, "[]\""), ", ")
}

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

// lastElement returns the last element of a SetNumber slice
func lastElement(slice []SetNumber) SetNumber {
	return slice[len(slice)-1]
}
