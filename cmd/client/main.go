package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mavleo96/cft-mavleo96/internal/config"
	pbBank "github.com/mavleo96/cft-mavleo96/pb/bank"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

// parseTransaction parses a transaction string of the format "(Sender, Receiver, Amount)"
func parseTransaction(s string) (pbBank.Transaction, error) {
	var p []string = strings.Split(strings.Trim(s, "()\""), ", ")

	amount, err := strconv.Atoi(p[2])
	if err != nil {
		return pbBank.Transaction{}, err
	}

	return pbBank.Transaction{
		Sender:   p[0],
		Receiver: p[1],
		Amount:   int64(amount)}, nil
}

// parseNodeString parses a string representation of a list of nodes
// func parseNodeString(s string) []string {
// 	return strings.Split(strings.Trim(s, "[]\""), ", ")
// }

func main() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	// log.SetLevel(log.FatalLevel)

	configPath := "./config.yaml"
	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatal(err)
	}
	var cfg config.Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	nodeClients := make(map[string]pbBank.TransactionServiceClient)
	for _, node := range cfg.Nodes {
		conn, err := grpc.NewClient(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		txnClient := pbBank.NewTransactionServiceClient(conn)
		nodeClients[node.ID] = txnClient
		defer conn.Close()
	}

	// Parse Transactions
	// The entire csv file is loaded into memory and transactions are queued by
	// client and set number like preserving the order for each client
	file, err := os.Open("testdata/test1.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// txnQueue is a nested map of client id to set number to list of transaction pointers
	txnQueue := make(map[string]map[int][]*pbBank.Transaction)
	for _, clientID := range cfg.Clients {
		txnQueue[clientID] = make(map[int][]*pbBank.Transaction)
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
				log.Fatal(err)
			}
			for _, clientID := range cfg.Clients {
				txnQueue[clientID][setNum] = make([]*pbBank.Transaction, 0)
			}

			// TODO: need to save alive nodes list here
		}

		// Parse and append transaction
		t, err := parseTransaction(record[1])
		if err != nil {
			log.Fatal(err)
		}
		txnQueue[t.Sender][setNum] = append(txnQueue[t.Sender][setNum], &t)
	}

	// Client Routines
	// Each client has its own goroutine and channel
	// The channel is used by main routine to inform which set to process next
	// and client uses it to inform main when it is done processing the set
	clientChannels := make(map[string]chan int)
	for _, clientID := range cfg.Clients {
		clientChannels[clientID] = make(chan int)
		defer close(clientChannels[clientID])
		go clientRoutine(clientID, clientChannels[clientID], txnQueue, nodeClients)
	}

	// Main Interactive Loop to control set processing
mainLoop:
	for i := 1; i <= setNum; i++ {
		// Signal all clients to process set i and wait for all to finish
		for _, clientID := range cfg.Clients {
			clientChannels[clientID] <- i
		}
		for _, clientID := range cfg.Clients {
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
				break mainLoop
			case "kill leader":
				fmt.Print(cmd)
				// TODO: implement kill leader
			case "print log":
				log.Info("print log command received")
				// TODO: implement print log
			case "print db":
				log.Info("print db command received")
				// TODO: implement print db
			case "print status":
				log.Info("print status command received")
				// TODO: implement print status
			case "print view":
				log.Info("print view command received")
				// TODO: implement print view
			default:
				continue interactionLoop
			}
		}
	}
	for _, clientID := range cfg.Clients {
		clientChannels[clientID] <- -1
	}
	log.Info("Exiting...")
}

func clientRoutine(clientID string, channel chan int, txnQueue map[string]map[int][]*pbBank.Transaction, nodeClients map[string]pbBank.TransactionServiceClient) {
	var leaderNode string = "n1"
	var clientTimer time.Duration = time.Second * 5

	type result struct {
		nodeID   string
		response *pbBank.TransferResponse
		err      error
	}

	for {
		setNum := <-channel
		if setNum == -1 {
			return
		}

		for i := range txnQueue[clientID][setNum] {
			t := txnQueue[clientID][setNum][i]
			timestamp := time.Now().UnixMilli()
			request := &pbBank.TransactionRequest{
				Transaction: t,
				Timestamp:   timestamp,
				Sender:      clientID,
			}
			resultsChan := make(chan result, len(nodeClients))

			var err error
			var response *pbBank.TransferResponse
			for attempt := 1; ; attempt++ {
				ctx, cancel := context.WithTimeout(context.Background(), clientTimer)
				if attempt == 1 {
					response, err = nodeClients[leaderNode].TransferRequest(ctx, request)
				} else {
					for nodeID, client := range nodeClients {
						go func(nodeID string, client pbBank.TransactionServiceClient) {
							resp, err := client.TransferRequest(ctx, request)
							resultsChan <- result{nodeID: nodeID, response: resp, err: err}
						}(nodeID, client)
					}

					for i := 0; i < len(nodeClients); i++ {
						res := <-resultsChan
						if res.err == nil {
							leaderNode = res.nodeID
							response = res.response
							err = nil
							log.Infof("Client %s updated leader to %s", clientID, leaderNode)
							break
						} else {
							err = res.err
						}
					}
				}
				cancel()
				if err == nil {
					log.Infof("Client %s processed transaction %v from %s with status %v on attempt %d", clientID, t.String(), leaderNode, response.Success, attempt)
					break
				} else {
					// log.Panic(err)
					log.Warnf("Client %s failed to process transaction %v from %s on attempt %d: %v", clientID, t.String(), leaderNode, attempt, err)
				}
			}
		}
		channel <- 0
	}
}
