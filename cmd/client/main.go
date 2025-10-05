package main

import (
	"fmt"
	"sync"

	"github.com/mavleo96/cft-mavleo96/internal/client"
	"github.com/mavleo96/cft-mavleo96/internal/config"
	bankpb "github.com/mavleo96/cft-mavleo96/pb/bank"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// log.SetLevel(log.FatalLevel)

	// Parse Config
	cfg, err := config.ParseConfig("./configs/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// Create gRPC clients for each node
	nodeClients := make(map[string]bankpb.TransactionServiceClient)
	for _, node := range cfg.Nodes {
		conn, err := grpc.NewClient(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// Skip if node is not reachable
			log.Warn(err)
			continue
		}
		txnClient := bankpb.NewTransactionServiceClient(conn)
		nodeClients[node.ID] = txnClient
		defer conn.Close()
	}

	// Parse Transactions
	// The entire csv file is loaded into memory and transactions are queued by
	// client and set number like preserving the order for each client.
	records, err := client.ReadCSV("./testdata/test1.csv")
	if err != nil {
		log.Fatal(err)
	}
	// masterQueue is a nested map of client id to set number to list of transactions.
	masterQueue, setNumList, err := client.CreateMasterQueue(records, cfg.Clients)
	if err != nil {
		log.Fatal(err)
	}

	// Client Routines
	// Each client has its own goroutine and channel. The channel is used by main routine and client routine to communicate.
	// - main routine sends set number to client routine
	// - client routine sends {nil, nil} to main routine when set is done
	// - main routine sends {-1, 0} to client routine to signal exit
	clientChannels := make(map[string]chan client.SetNumber)
	for _, clientID := range cfg.Clients {
		clientChannels[clientID] = make(chan client.SetNumber)
		defer close(clientChannels[clientID])
		go client.ClientRoutine(clientID, clientChannels[clientID], masterQueue[clientID], nodeClients)
	}

	// Main Interactive Loop to control set processing
mainLoop:
	for i, setNum := range setNumList {
		if setNum.N2 == 1 {
			// TODO: Signal dead nodes
		}
		// Signal all clients to process set and wait for all to finish
		wg := sync.WaitGroup{}
		for _, clientID := range cfg.Clients {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				clientChannels[id] <- setNum
				<-clientChannels[id]
			}(clientID)
		}
		wg.Wait()
		// if next set number N1 is same then continue
		if i < len(setNumList)-1 && setNumList[i+1].N1 == setNum.N1 {
			continue
		}
		log.Infof("Set %d processed by all clients", setNum.N1)

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
			case "print log":
				log.Info("Print log command received")
				// TODO: implement print log
			case "print db":
				log.Info("Print db command received")
				// TODO: implement print db
			case "print status":
				log.Info("Print status command received")
				// TODO: implement print status
			case "print view":
				log.Info("Print view command received")
				// TODO: implement print view
			default:
				continue interactionLoop
			}
		}
	}
	for _, clientID := range cfg.Clients {
		clientChannels[clientID] <- client.SetNumber{N1: -1, N2: 0}
	}
	log.Info("Exiting...")
}
