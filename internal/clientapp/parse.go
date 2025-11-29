package clientapp

import (
	"encoding/csv"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/internal/utils"
	pb "github.com/mavleo96/stable-leader-paxos/pb"
)

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

// ParseRecords parses the records from a csv file and returns a list of test sets
func ParseRecords(records [][]string, nodeMap map[string]*models.Node, clientIDs []string) ([]*TestSet, error) {
	testSets := make([]*TestSet, 0)

	// Process records
	for i, record := range records {
		if i == 0 {
			continue // skip header row
		}

		// If set number is new, create new test set
		if record[0] != "" {
			// Parse set number
			setNumber, err := strconv.ParseInt(record[0], 10, 64)
			if err != nil {
				return []*TestSet{}, err
			}

			liveNodes := parseNodeString(record[2], nodeMap)
			emptyTransactionList := makeEmptyTransactionListMap(clientIDs)
			testSets = append(testSets, &TestSet{
				SetNumber:         setNumber,
				TransactionsLists: []TransactionListMap{emptyTransactionList},
				Overrides:         make([]Override, 0),
				Live:              liveNodes,
			})

		}
		currentTestSet := utils.LastElement(testSets)

		// If record first element is not "(" then it is an override
		if record[1][0] != '(' {
			override, err := parseOverrideString(record[1])
			if err != nil {
				return []*TestSet{}, err
			}
			currentTestSet.Overrides = append(currentTestSet.Overrides, override)

			// Initialize new transaction list for the next leg of transactions
			emptyTransactionList := makeEmptyTransactionListMap(clientIDs)
			currentTestSet.TransactionsLists = append(currentTestSet.TransactionsLists, emptyTransactionList)
			continue
		}

		// Parse and append transaction
		t, err := parseTransactionString(record[1])
		if err != nil {
			return []*TestSet{}, err
		}
		lastIdx := len(currentTestSet.TransactionsLists) - 1
		currentTransactionList := currentTestSet.TransactionsLists[lastIdx]
		currentTransactionList[t.Sender] = append(currentTransactionList[t.Sender], &t)
	}
	return testSets, nil
}

func makeEmptyTransactionListMap(clientIDs []string) TransactionListMap {
	transactionListMap := make(TransactionListMap)
	for _, clientID := range clientIDs {
		transactionListMap[clientID] = make([]*pb.Transaction, 0)
	}
	return transactionListMap
}

// parseTransactionString parses a transaction string of the format "(Sender, Receiver, Amount)"
func parseTransactionString(s string) (pb.Transaction, error) {
	p := strings.Split(strings.Trim(s, "()\""), ", ")

	if len(p) != 3 {
		return pb.Transaction{}, errors.New("invalid transaction string: " + s)
	}

	amount, err := strconv.ParseInt(p[2], 10, 64)
	if err != nil {
		return pb.Transaction{}, err
	}

	return pb.Transaction{
		Sender:   p[0],
		Receiver: p[1],
		Amount:   amount,
	}, nil
}

// parseNodeString parses a string representation of a list of nodes of the format "[n1, n2, n3]"
func parseNodeString(s string, nodeMap map[string]*models.Node) []*models.Node {
	nodes := make([]*models.Node, 0)

	cleanedString := strings.Trim(s, "[]\"")
	if cleanedString == "" {
		return nodes
	}

	for _, n := range strings.Split(cleanedString, ", ") {
		if node, exists := nodeMap[n]; exists {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// parseOverrideString
func parseOverrideString(s string) (Override, error) {
	cleanedString := strings.Trim(s, ", ")
	if cleanedString == "" {
		return Override{}, errors.New("invalid override string: " + s)
	}

	overrideOp := ""
	switch cleanedString {
	case "LF":
		overrideOp = "kill leader"
	default:
		return Override{}, errors.New("invalid override string: " + s)
	}

	return Override{
		OverrideType: overrideOp,
	}, nil
}
