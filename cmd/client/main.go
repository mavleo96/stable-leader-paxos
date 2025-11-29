package main

import (
	"bufio"
	"context"
	"flag"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/mavleo96/stable-leader-paxos/internal/clientapp"
	"github.com/mavleo96/stable-leader-paxos/internal/config"
	"github.com/mavleo96/stable-leader-paxos/internal/models"
	"github.com/mavleo96/stable-leader-paxos/pb"
	log "github.com/sirupsen/logrus"
)

func main() {
	// log.SetLevel(log.FatalLevel)
	filePath := flag.String("file", "testdata/test.csv", "Path to CSV file")
	flag.Parse()

	// Parse Config
	cfg, err := config.ParseConfig("./configs/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// Create map of node structs
	// Note: this object is shared by clients
	nodeMap, err := models.GetNodeMap(cfg.Nodes)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize leader node - try n1 first, otherwise use first available node
	leaderNode := "n1"
	if _, exists := nodeMap[leaderNode]; !exists {
		// If n1 doesn't exist, use the first available node
		for nodeID := range nodeMap {
			leaderNode = nodeID
			break
		}
	}
	log.Infof("Leader node initialized to %s", leaderNode)

	// Parse Transactions
	// The entire csv file is loaded into memory and transactions are queued by
	// client and set number like preserving the order for each client.
	records, err := clientapp.ReadCSV(*filePath)
	if err != nil {
		log.Fatal(err)
	}
	testSets, err := clientapp.ParseRecords(records, nodeMap, cfg.Clients)
	if err != nil {
		log.Fatal(err)
	}

	// Create main context and wait group
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := sync.WaitGroup{}

	// Create channels
	testSetCh := make(chan clientapp.TestSet)                 // Channel for main routine to send test sets to coordinator
	signalCh := make(chan bool, len(cfg.Clients))             // Channel for client routines to send signals to coordinator
	clientChannels := make(map[string]chan []*pb.Transaction) // Channel for coordinator to send transactions to clients
	resetChannels := make(map[string]chan bool)               // Channel for main routine to send reset signals to clients
	for _, clientID := range cfg.Clients {
		// Use buffered channel to prevent blocking if client routine is busy
		clientChannels[clientID] = make(chan []*pb.Transaction, 1)
		resetChannels[clientID] = make(chan bool, 1)
	}

	// Client Routines
	// Each client has its own goroutine and channel. The channel is used by main routine and client routine to communicate.
	// - main routine sends set number to client routine
	// - client routine sends {nil, nil} to main routine when set is done
	for _, clientID := range cfg.Clients {
		wg.Go(func() {
			clientapp.ClientRoutine(ctx, clientID, leaderNode, clientChannels[clientID], resetChannels[clientID], signalCh, nodeMap)
		})

	}

	// Coordinator routine to coordinate the test sets
	wg.Go(func() { clientapp.Coordinator(ctx, nodeMap, testSetCh, clientChannels, signalCh) })

	// Main interaction loop
	// This loop is used to interact with the user and execute commands
	// to control the execution of the test sets and log the results.
	log.Info("Main interaction loop started")
	scanner := bufio.NewScanner(os.Stdin)
	testSetIndex := int64(-1)
interactionLoop:
	for {
		// Read command from stdin
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				log.Panic(err)
			}
		}
		cmd := strings.TrimSpace(scanner.Text())

		// If command contains "print status", then parse the argument
		var arg int64
		if strings.HasPrefix(cmd, "print status") {
			var err error
			n := strings.TrimPrefix(cmd, "print status")
			n = strings.TrimSpace(n)
			if n == "all" {
				arg = int64(0)
			} else {
				arg, err = strconv.ParseInt(n, 10, 64)
			}
			if err != nil {
				log.Warn(err)
				continue interactionLoop
			}
			cmd = "print status"
			if arg < 0 {
				log.Warn("Invalid argument for print status")
				continue interactionLoop
			}
		}

		// Execute command
		switch cmd {
		case "next":
			testSetIndex++
			if testSetIndex == int64(len(testSets)) {
				break interactionLoop
			}
			testSetCh <- *testSets[testSetIndex]
		case "print log":
			clientapp.SendPrintLogCommand(nodeMap, testSetIndex)
		case "print db":
			clientapp.SendPrintDBCommand(nodeMap, testSetIndex)
		case "print status":
			clientapp.SendPrintStatusCommand(nodeMap, testSetIndex, arg)
		case "print view":
			clientapp.SendPrintViewCommand(nodeMap, testSetIndex)
		case "kill leader":
			clientapp.KillLeader(nodeMap)
		case "reset":
			for _, clientID := range cfg.Clients {
				resetChannels[clientID] <- true
			}
			clientapp.SendResetCommand(nodeMap)
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
