package client

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"

	bankpb "github.com/mavleo96/cft-mavleo96/pb/bank"
)

// ParseTransactionString parses a transaction string of the format "(Sender, Receiver, Amount)"
func ParseTransactionString(s string) (bankpb.Transaction, error) {
	var p []string = strings.Split(strings.Trim(s, "()\""), ", ")

	amount, err := strconv.Atoi(p[2])
	if err != nil {
		return bankpb.Transaction{}, err
	}

	return bankpb.Transaction{
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
