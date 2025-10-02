package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var clients []string = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}

// transaction represents the structure of transaction to be processed
type transaction struct {
	Sender   string
	Receiver string
	Amount   int
}

// parseTransaction parses a transaction string of the format "(Sender, Receiver, Amount)"
func parseTransaction(s string) (transaction, error) {
	var p []string = strings.Split(strings.Trim(s, "()\""), ", ")
	amount, err := strconv.Atoi(p[2])
	if err != nil {
		return transaction{}, err
	}

	return transaction{p[0], p[1], amount}, nil
}

// parseNodeString parses a string representation of a list of nodes
// func parseNodeString(s string) []string {
// 	return strings.Split(strings.Trim(s, "[]\""), ", ")
// }

func main() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

	// Parse Transactions
	// The entire csv file is loaded into memory and transactions are queued by
	// client and set number like preserving the order for each client
	file, err := os.Open("test1.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// txnQueue is a nested map of client id to set number to list of transactions
	txnQueue := make(map[string]map[int][]transaction)
	for _, clientID := range clients {
		txnQueue[clientID] = make(map[int][]transaction)
	}

	// Process records
	var setNum int
	for i, record := range records {
		if i == 0 {
			continue // skip header row
		}

		// If set number is new, initialize new lists for each client
		if record[0] != "" {
			var err error
			setNum, err = strconv.Atoi(record[0])
			if err != nil {
				fmt.Println("error parsing set number:", err)
			}
			for _, clientID := range clients {
				txnQueue[clientID][setNum] = make([]transaction, 0)
			}

			// TODO: need to save alive nodes list here
		}

		// Parse and append transaction
		t, err := parseTransaction(record[1])
		if err != nil {
			log.Fatal(err)
		}
		txnQueue[t.Sender][setNum] = append(txnQueue[t.Sender][setNum], t)
	}

	// Client Routines
	// Each client has its own goroutine and channel
	// The channel is used by main routine to inform which set to process next
	// and client uses it to inform main when it is done processing the set
	clientChannels := make(map[string]chan int)
	for _, clientID := range clients {
		clientChannels[clientID] = make(chan int)
		defer close(clientChannels[clientID])
		go clientRoutine(clientID, clientChannels[clientID], txnQueue)
	}

	// Main Interactive Loop to control set processing
	for i := 1; i <= setNum; i++ {
		// Signal all clients to process set i and wait for all to finish
		for _, clientID := range clients {
			clientChannels[clientID] <- i
		}
		for _, clientID := range clients {
			<-clientChannels[clientID]
		}
		log.Info("Set ", i, " processed by all clients")

		// Interaction loop
		var cmd string
	interactionLoop:
		for {
			_, err := fmt.Scan(&cmd)
			if err != nil {
				log.Panic(err)
			}
			switch cmd {
			case "next":
				break interactionLoop
			case "exit":
				log.Info("Exiting...")
				return
			case "kill leader":
				fmt.Print(cmd)
				// TODO: implement kill leader
			case "print log":
				// TODO: implement print log
			case "print db":
				// TODO: implement print db
			case "print status":
				// TODO: implement print status
			case "print view":
				// TODO: implement print view
			default:
				continue
			}
		}
	}
	// TODO: implement graceful shutdown
}

func clientRoutine(clientID string, channel chan int, txnQueue map[string]map[int][]transaction) {
	for {
		setNum := <-channel

		for _, t := range txnQueue[clientID][setNum] {
			log.Infof("Client %s processing transaction: %v", clientID, t)
			time.Sleep(500 * time.Millisecond) // simulate processing time
		}
		channel <- 0
	}
}
