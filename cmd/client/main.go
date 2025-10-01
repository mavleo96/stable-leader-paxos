package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	models "github.com/mavleo96/cft-mavleo96/internal/models"
)

var clients [10]string = [10]string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}

func main() {
	fmt.Println("DEBUG: client application started...")

	fmt.Println("DEBUG: spawning clients...")
	clientChannels := make(map[string]chan models.Transaction)
	for _, client_id := range clients {
		// TODO: review if a bounded channel buffer is the correct design choice?
		clientChannels[client_id] = make(chan models.Transaction, 10)
		go clientRoutine(client_id, clientChannels[client_id])
		defer close(clientChannels[client_id])
	}

	file, err := os.Open("test1.csv")
	if err != nil {
		fmt.Println("error opening file:", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("error reading records:", err)
	}

	for i, record := range records {
		if i == 0 {
			continue
		}
		if record[0] != "" {
			fmt.Scanln()
			fmt.Printf("DEBUG: processing set number %s...\n", record[0])

			nodeList := parseNodeString(record[2])
			fmt.Printf("DEBUG: alive nodes %v...\n", nodeList)
			// TODO: need to send signals to stop inter node communication?
		}
		transaction, err := models.ParseTransaction(record[1])
		if err != nil {
			fmt.Println("error parsing transaction:", err)
		}
		clientChannels[transaction.Sender] <- transaction
	}
	// TODO: review what to do when main has exhausted parsing records
	// wait for channel goroutines to finish or close on prompt
	fmt.Scanln()
}

func parseNodeString(s string) []string {
	return strings.Split(strings.Trim(s, "[]\""), ", ")
}

func clientRoutine(client_id string, channel <-chan models.Transaction) {
	fmt.Printf("DEBUG: client %s running...\n", client_id)
	for {
		transaction := <-channel
		fmt.Printf("DEBUG: client %s processing transaction %v at %d\n", client_id, transaction, time.Now().UnixNano())
		if client_id != "A" {
			// emulating processing for now
			time.Sleep(time.Second)
		}
	}
}
