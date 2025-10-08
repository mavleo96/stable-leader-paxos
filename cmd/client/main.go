package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/mavleo96/cft-mavleo96/internal/client"
	"github.com/mavleo96/cft-mavleo96/internal/config"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	// log.SetLevel(log.FatalLevel)
	filePath := flag.String("file", "", "Path to CSV file")
	flag.Parse()

	// Parse Config
	cfg, err := config.ParseConfig("./configs/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// Create gRPC clients for each node
	nodeClients := make(map[string]pb.PaxosClient)
	for _, node := range cfg.Nodes {
		var conn *grpc.ClientConn
		var err error
		for {
			conn, err = grpc.NewClient(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Warn(err)
				continue
			}
			break
		}
		txnClient := pb.NewPaxosClient(conn)
		nodeClients[node.ID] = txnClient
		defer conn.Close()
	}

	// Parse Transactions
	// The entire csv file is loaded into memory and transactions are queued by
	// client and set number like preserving the order for each client.
	records, err := client.ReadCSV(*filePath)
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
	clientChannels := make(map[string]chan client.SetNumber)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, clientID := range cfg.Clients {
		clientChannels[clientID] = make(chan client.SetNumber)
		go client.ClientRoutine(ctx, clientID, clientChannels[clientID], masterQueue[clientID], nodeClients)
	}

	scanner := bufio.NewScanner(os.Stdin)

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
	interactionLoop:
		for {
			fmt.Print("> ")
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					log.Panic(err)
				}
			}
			cmd := strings.TrimSpace(scanner.Text())
			switch cmd {
			case "next":
				break interactionLoop
			case "exit":
				break mainLoop
			case "print log":
				log.Info("Print log command received")
				// TODO: implement print log
				for _, nodeClient := range nodeClients {
					_, err = nodeClient.PrintLog(context.Background(), &emptypb.Empty{})
					if err != nil {
						log.Panic(err)
					}
				}
			case "print db":
				log.Info("Print db command received")
				// TODO: implement print db
				for _, nodeClient := range nodeClients {
					_, err = nodeClient.PrintDB(context.Background(), &emptypb.Empty{})
					if err != nil {
						log.Panic(err)
					}
				}
			case "print status":
				log.Info("Print status command received")
				// TODO: implement print status
			case "print view":
				log.Info("Print view command received")
				// TODO: implement print view
			case "print timer state":
				log.Info("Print timer state command received")
				// TODO: implement print timer state
				for _, nodeClient := range nodeClients {
					_, err = nodeClient.PrintTimerState(context.Background(), &emptypb.Empty{})
					if err != nil {
						log.Panic(err)
					}
				}
			default:
				continue interactionLoop
			}
		}
	}
	cancel()
	log.Info("Exiting...")
}
