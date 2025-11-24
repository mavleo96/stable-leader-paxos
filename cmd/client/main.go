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
	log "github.com/sirupsen/logrus"
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

	// Create map of node structs
	// Note: this object is shared by clients
	nodeMap, err := models.GetNodeMap(cfg.Nodes)
	if err != nil {
		log.Fatal(err)
	}

	// Parse Transactions
	// The entire csv file is loaded into memory and transactions are queued by
	// client and set number like preserving the order for each client.
	records, err := clientapp.ReadCSV(*filePath)
	if err != nil {
		log.Fatal(err)
	}
	// masterQueue is a nested map of client id to set number to list of transactions.
	masterQueue, setNumList, aliveNodesMap, err := clientapp.ParseRecords(records, cfg.Clients)
	if err != nil {
		log.Fatal(err)
	}

	// Client Routines
	// Each client has its own goroutine and channel. The channel is used by main routine and client routine to communicate.
	// - main routine sends set number to client routine
	// - client routine sends {nil, nil} to main routine when set is done
	wg := sync.WaitGroup{}
	clientChannels := make(map[string]chan clientapp.SetNumber)
	resetChannels := make(map[string]chan bool)
	for _, clientID := range cfg.Clients {
		clientChannels[clientID] = make(chan clientapp.SetNumber)
		resetChannels[clientID] = make(chan bool)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, clientID := range cfg.Clients {
		wg.Add(1)
		go func(ctx context.Context, id string) {
			defer wg.Done()
			clientapp.ClientRoutine(ctx, id, clientChannels[id], resetChannels[id], masterQueue[id], nodeMap)
		}(ctx, clientID)

	}
	queueChan := make(chan clientapp.SetNumber, 5)
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientapp.QueueRoutine(ctx, queueChan, clientChannels, nodeMap)
	}()

	// Main interaction loop
	// This loop is used to interact with the user and execute commands
	// to control the execution of the test sets and log the results.
	log.Info("Main interaction loop started")
	scanner := bufio.NewScanner(os.Stdin)
	setNum := clientapp.SetNumber{N1: 0, N2: 0}
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
		var arg int
		if strings.HasPrefix(cmd, "print status") {
			var err error
			n := strings.TrimPrefix(cmd, "print status")
			n = strings.TrimSpace(n)
			if n == "all" {
				arg = 0
			} else {
				arg, err = strconv.Atoi(n)
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
			// send set number to queue routine
			// var setNum clientapp.SetNumber
			for {
				if len(setNumList) == 0 {
					break interactionLoop
				} else if len(setNumList) > 1 {
					setNum, setNumList = setNumList[0], setNumList[1:]
				} else {
					setNum, setNumList = setNumList[0], make([]clientapp.SetNumber, 0)
				}
				if setNum.N2 == 1 {
					clientapp.ReconfigureNodes(nodeMap, aliveNodesMap[setNum])
				}
				queueChan <- setNum
				if len(setNumList) > 0 && setNumList[0].N2 > 1 {
					continue
				}
				break
			}
		case "print log":
			clientapp.SendPrintLogCommand(nodeMap, setNum.N1)
		case "print db":
			clientapp.SendPrintDBCommand(nodeMap, setNum.N1)
		case "print status":
			clientapp.SendPrintStatusCommand(nodeMap, setNum.N1, setNum.N2)
		case "print view":
			clientapp.SendPrintViewCommand(nodeMap, setNum.N1)
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
