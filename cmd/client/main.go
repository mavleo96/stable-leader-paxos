package main

import (
	"bufio"
	"context"
	"flag"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/mavleo96/cft-mavleo96/internal/client"
	"github.com/mavleo96/cft-mavleo96/internal/config"
	pb "github.com/mavleo96/cft-mavleo96/pb/paxos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
	masterQueue, setNumList, aliveNodesMap, err := client.ParseRecords(records, cfg.Clients)
	if err != nil {
		log.Fatal(err)
	}

	// Client Routines
	// Each client has its own goroutine and channel. The channel is used by main routine and client routine to communicate.
	// - main routine sends set number to client routine
	// - client routine sends {nil, nil} to main routine when set is done
	wg := sync.WaitGroup{}
	clientChannels := make(map[string]chan client.SetNumber)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, clientID := range cfg.Clients {
		clientChannels[clientID] = make(chan client.SetNumber)
		wg.Add(1)
		go func(ctx context.Context, id string) {
			defer wg.Done()
			client.ClientRoutine(ctx, id, clientChannels[id], masterQueue[id], nodeClients)
		}(ctx, clientID)

	}
	queueChan := make(chan client.SetNumber, 5)
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.QueueRoutine(ctx, queueChan, clientChannels, nodeClients)
	}()

	scanner := bufio.NewScanner(os.Stdin)

interactionLoop:
	for {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				log.Panic(err)
			}
		}
		cmd := strings.TrimSpace(scanner.Text())
		var arg int
		if strings.HasPrefix(cmd, "print status") {
			var err error
			n := strings.TrimPrefix(cmd, "print status")
			n = strings.TrimSpace(n)
			arg, err = strconv.Atoi(n)
			if err != nil {
				log.Warn(err)
				continue interactionLoop
			}
			cmd = "print status"
			if arg <= 0 {
				log.Warn("Invalid argument for print status")
				continue interactionLoop
			}
		}
		// If command contains "print status", then
		switch cmd {
		case "next":
			// send set number to queue routine
			var setNum client.SetNumber
			for {
				if len(setNumList) == 0 {
					break interactionLoop
				} else if len(setNumList) > 1 {
					setNum, setNumList = setNumList[0], setNumList[1:]
				} else {
					setNum, setNumList = setNumList[0], make([]client.SetNumber, 0)
				}
				if setNum.N2 == 1 {
					client.ReconfigureNodes(aliveNodesMap[setNum], nodeClients)
				}
				queueChan <- setNum
				if len(setNumList) > 0 && setNumList[0].N2 > 1 {
					continue
				}
				break
			}
		case "print log":
			log.Info("Print log command received")
			for _, nodeClient := range nodeClients {
				_, err = nodeClient.PrintLog(context.Background(), &emptypb.Empty{})
				if err != nil {
					log.Warn(err)
				}
			}
		case "print db":
			log.Info("Print db command received")
			for _, nodeClient := range nodeClients {
				_, err = nodeClient.PrintDB(context.Background(), &emptypb.Empty{})
				if err != nil {
					log.Warn(err)
				}
			}
		case "print status":
			log.Infof("Print status command received for %d", arg)
			for _, nodeClient := range nodeClients {
				_, err = nodeClient.PrintStatus(context.Background(), &wrapperspb.Int64Value{Value: int64(arg)})
				if err != nil {
					log.Warn(err)
				}
			}
		case "print view":
			log.Info("Print view command received")
			for _, nodeClient := range nodeClients {
				_, err = nodeClient.PrintView(context.Background(), &emptypb.Empty{})
				if err != nil {
					log.Warn(err)
				}
			}
		case "kill leader":
			log.Info("lf command received")
			for _, nodeClient := range nodeClients {
				_, err = nodeClient.KillLeader(context.Background(), &emptypb.Empty{})
				if err != nil {
					log.Warn(err)
				}
			}
		case "exit":
			break interactionLoop
		default:
			continue interactionLoop
		}
	}
	cancel()
	wg.Wait()
	log.Info("Client main routine exiting...")
}
