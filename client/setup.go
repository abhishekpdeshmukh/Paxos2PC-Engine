package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
)

func readTransactions() {
	// Open the CSV file
	file, err := os.Open("../test.csv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error reading CSV file:", err)
		return
	}

	var currentSet TransactionSet

	for _, record := range records {
		// Check if the first field is not empty, indicating a new set
		if record[0] != "" {
			// If there is a current set, append it to the list
			if currentSet.setID != 0 {
				transactionSets = append(transactionSets, currentSet)
			}

			// Parse the new set
			setNumber, _ := strconv.Atoi(record[0])
			currentSet = TransactionSet{
				setID: setNumber,
			}

			// Parse the first transaction
			transaction := parseProtoTransaction(record[1])
			currentSet.transactions = []*pb.Transaction{transaction}

			// Parse the live servers
			currentSet.liveServers = parseLiveServers(record[2])
		} else {
			// Parse subsequent transactions for the current set
			transaction := parseProtoTransaction(record[1])
			currentSet.transactions = append(currentSet.transactions, transaction)
		}
	}

	// Append the last set
	if currentSet.setID != 0 {
		transactionSets = append(transactionSets, currentSet)
	}

	// Example: Print the parsed sets
	// for _, set := range transactionSets {
	// 	fmt.Printf("Set Number: %d\n", set.setID)
	// 	fmt.Printf("Live Servers: %v\n", set.liveServers)
	// 	fmt.Println("Transactions:")
	// 	for _, txn := range set.transactions {
	// 		fmt.Printf("Sender: %d, Receiver: %d, Amount: %d\n", txn.Sender, txn.Receiver, txn.Amount)
	// 	}
	// 	fmt.Println()
	// }
}

func readConfig(configFile string) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		fmt.Println("Error reading config file:", err)
		return
	}

	// Parse the JSON into the Config struct
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}
	for _, cluster := range config.Clusters {
		clusterID := cluster.ID

		// Populate dataItemToCluster
		if cluster.Shard.Range != "" {
			start, end := parseRange(cluster.Shard.Range)
			for i := start; i <= end; i++ {
				dataItemToCluster[i] = clusterID
			}
		}
		if cluster.Shard.ExplicitIds != nil {
			for _, id := range cluster.Shard.ExplicitIds {
				dataItemToCluster[id] = clusterID
			}
		}
		
		// Populate clusterToServers
		clusterToServers[clusterID] = cluster.Servers
	}

}
