package main

import (
	"fmt"
	"strconv"
	"strings"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
)

func sendNextTransactionSet() {
	// Check if there are more sets to send
	if currentSetIndex >= len(transactionSets) {
		fmt.Println("No more transaction sets to send.")
		return
	}

	// Get the current transaction set
	currentSet := transactionSets[currentSetIndex]
	fmt.Printf("Sending transactions for Set Number: %d\n", currentSet.setID)

	// Placeholder: Kill servers not in the live set
	killInactiveServers(currentSet.liveServers)

	// Placeholder: Revive servers in the live set
	reviveActiveServers(currentSet.liveServers)

	// Placeholder: Send transactions via RPC
	for _, txn := range currentSet.transactions {
		fmt.Printf("Sending transaction: Sender: %d, Receiver: %d, Amount: %d\n",
			txn.Sender, txn.Receiver, txn.Amount)
		// Call your RPC function to send the transaction
		// e.g., sendTransactionRPC(txn)
	}

	// Move to the next set
	currentSetIndex++
}

func isCrossShard(x, y int) bool {
	return dataItemToCluster[x] != dataItemToCluster[y]
}

func parseProtoTransaction(transactionStr string) *pb.Transaction {
	transactionStr = strings.Trim(transactionStr, "()")
	parts := strings.Split(transactionStr, ",")
	sender, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
	receiver, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
	amount, _ := strconv.Atoi(strings.TrimSpace(parts[2]))
	return &pb.Transaction{
		Sender:   int32(sender),
		Receiver: int32(receiver),
		Amount:   int32(amount),
	}
}

// Helper function to parse live servers
func parseLiveServers(liveServersStr string) []int {
	liveServersStr = strings.Trim(liveServersStr, "[]")
	parts := strings.Split(liveServersStr, ",")
	var liveServers []int
	for _, part := range parts {
		server, _ := strconv.Atoi(strings.TrimSpace(part))
		liveServers = append(liveServers, server)
	}
	return liveServers
}

func parseRange(rangeStr string) (int, int) {
	parts := strings.Split(rangeStr, "-")
	start, _ := strconv.Atoi(parts[0])
	end, _ := strconv.Atoi(parts[1])
	return start, end
}
