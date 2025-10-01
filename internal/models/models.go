package models

import (
	"strconv"
	"strings"
)

type Transaction struct {
	Sender   string
	Receiver string
	Amount   int
}

func ParseTransaction(s string) (Transaction, error) {
	var p []string = strings.Split(strings.Trim(s, "()\""), ", ")
	amount, err := strconv.Atoi(p[2])
	if err != nil {
		return Transaction{}, err
	}

	return Transaction{p[0], p[1], amount}, nil
}
