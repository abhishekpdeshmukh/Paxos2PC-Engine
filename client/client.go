package main

import (
	"fmt"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var dataItemToCluster = make(map[int]int)
var clusterToServers = make(map[int][]Server)
var transactionSets []TransactionSet
var currentSetIndex int
var globalTransactionID = 0
var serverIDToClusterID = make(map[int]int)
var clusterIDs []int

type TransactionSet struct {
	setID        int
	transactions []*pb.Transaction
	liveServers  []int
	leaders      []int // Added leaders field
}

// Parse the CSV records

func main() {
	readConfig("../config.json")
	readTransactions()
	for {
		// Display menu options
		fmt.Println("\nMENU:")
		fmt.Println("1. PrintBalance")
		fmt.Println("2. PrintLog")
		fmt.Println("3. Performance")
		fmt.Println("4. Send transaction from next Set")
		fmt.Println("5. Clear DB")

		// Get user input
		var choice int
		fmt.Print("Enter your choice: ")
		_, err := fmt.Scan(&choice)
		if err != nil {
			fmt.Println("Invalid input. Please enter a number between 1 and 5.")
			continue
		}

		// Handle menu options
		switch choice {
		case 1:
			fmt.Println("Executing PrintBalance...")
			PrintBalance()
		case 2:
			fmt.Println("Executing PrintLog...")

			PrintDatastore()
		case 3:
			fmt.Println("Executing Performance...")

		case 4:
			fmt.Println("Reading transaction from next Set...")

			sendNextTransactionSet()
		case 5:
			fmt.Println("Clearing DB...")
			ClearDB()
		default:
			fmt.Println("Invalid choice. Please enter a number between 1 and 5.")
		}
	}
}

// Placeholder function to kill inactive servers
func killInactiveServers(liveServers []int) {
	fmt.Println("Killing servers not in the live set...")
	liveSet := make(map[int]bool)
	for _, server := range liveServers {
		liveSet[server] = true
	}

	for i := 1; i <= len(liveServers); i++ {
		if !liveSet[i] {
			fmt.Println(i)
			c, ctx, conn := setUpClientServerRPC(i)
			c.Kill(ctx, &emptypb.Empty{})
			conn.Close()
		}
	}
}
func ClearDB() {
	for i := range clusterIDs {
		fmt.Println(clusterToServers[i])
		for _, server := range clusterToServers[i] {
			c, ctx, conn := setUpClientServerRPC(server.ServerID)
			c.ClearDB(ctx, &emptypb.Empty{})
			conn.Close()
		}
	}
}

// Placeholder function to revive active servers
func reviveActiveServers(liveServers []int) {
	fmt.Println("Reviving servers in the live set...")
	// Placeholder logic: You would implement the RPC call to revive servers here
	// e.g., for each server in liveServers, call reviveServerRPC(serverID)

	for _, i := range liveServers {
		c, ctx, conn := setUpClientServerRPC(i)
		c.Revive(ctx, &emptypb.Empty{})
		conn.Close()
	}

}
