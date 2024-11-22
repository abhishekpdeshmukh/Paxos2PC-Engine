package main

import (
	"fmt"
	"log"
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

	// Build mapping from cluster IDs to leader server IDs
	clusterIDToLeaderServerID := make(map[int]int)
	for _, leaderServerID := range currentSet.leaders {
		clusterID := serverIDToClusterID[leaderServerID]
		clusterIDToLeaderServerID[clusterID] = leaderServerID
	}

	// Placeholder: Kill servers not in the live set
	killInactiveServers(currentSet.liveServers)

	// Placeholder: Revive servers in the live set
	reviveActiveServers(currentSet.liveServers)

	// Send transactions via RPC
	for _, txn := range currentSet.transactions {
		fmt.Printf("Processing transaction: Sender: %d, Receiver: %d, Amount: %d\n",
			txn.Sender, txn.Receiver, txn.Amount)
		go func(txn *pb.Transaction) {
			if isCrossShard(txn) {
				fmt.Println("Processing CROSS Shard transaction:", txn)
				handleCrossShardTransaction(txn, clusterIDToLeaderServerID)
			} else {
				fmt.Println("Processing INTRA Shard transaction:", txn)
				sendIntraShardTransaction(txn, clusterIDToLeaderServerID)
			}
		}(txn)
	}

	// Move to the next set
	currentSetIndex++
}

func isCrossShard(transaction *pb.Transaction) bool {
	return dataItemToCluster[int(transaction.Sender)] != dataItemToCluster[int(transaction.Receiver)]
}

func parseProtoTransaction(transactionStr string) *pb.Transaction {
	transactionStr = strings.Trim(transactionStr, "()")
	parts := strings.Split(transactionStr, ",")
	sender, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
	receiver, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
	amount, _ := strconv.Atoi(strings.TrimSpace(parts[2]))
	globalTransactionID += 1
	return &pb.Transaction{
		Id:       int32(globalTransactionID),
		Sender:   int32(sender),
		Receiver: int32(receiver),
		Amount:   int32(amount),
	}
}

// Helper function to parse live servers
func parseLiveServers(liveServersStr string) []int {
	fmt.Println(liveServersStr)
	liveServersStr = strings.Trim(liveServersStr, "[]")
	parts := strings.Split(liveServersStr, ",")
	var liveServers []int
	for _, part := range parts {
		// fmt.Println(string(strings.TrimSpace(part)[1]))
		server, _ := strconv.Atoi(string(strings.TrimSpace(part)[1]))
		liveServers = append(liveServers, server)
	}
	fmt.Println("These are the live servers I have ")
	fmt.Println(liveServers)
	return liveServers
}

func parseRange(rangeStr string) (int, int) {
	parts := strings.Split(rangeStr, "-")
	start, _ := strconv.Atoi(parts[0])
	end, _ := strconv.Atoi(parts[1])
	return start, end
}

func handleCrossShardTransaction(txn *pb.Transaction, clusterIDToLeaderServerID map[int]int) {
	// Get shards for sender and receiver
	// senderShardID := dataItemToCluster[int(txn.Sender)]
	// receiverShardID := dataItemToCluster[int(txn.Receiver)]

	// // Get server addresses for sender and receiver shards
	// senderServer := clusterToServers[senderShardID][0]     // Assuming first server
	// receiverServer := clusterToServers[receiverShardID][0] // Assuming first server

	// // Set up RPC connections
	// senderClient, senderCtx, senderConn := setUpClientServerRPC(senderServer.ServerID)
	// defer senderConn.Close()

	// receiverClient, receiverCtx, receiverConn := setUpClientServerRPC(receiverServer.ServerID)
	// defer receiverConn.Close()

	// // Channels for responses
	// prepareChan := make(chan bool, 2)

	// // Send Prepare to both shards
	// go func() {
	// 	resp, err := senderClient.Prepare(senderCtx, &pb.ClientPrepare{Transaction: txn})
	// 	if err != nil {
	// 		log.Printf("Error sending Prepare to sender shard: %v", err)
	// 		prepareChan <- false
	// 		return
	// 	}
	// 	prepareChan <- resp.CanCommit
	// }()

	// go func() {
	// 	resp, err := receiverClient.Prepare(receiverCtx, &pb.ClientPrepare{Transaction: txn})
	// 	if err != nil {
	// 		log.Printf("Error sending Prepare to receiver shard: %v", err)
	// 		prepareChan <- false
	// 		return
	// 	}
	// 	prepareChan <- resp.CanCommit
	// }()

	// // Wait for responses with timeout
	// timeout := time.After(30 * time.Second) // Adjust as needed
	// canCommitCount := 0

	// for i := 0; i < 2; i++ {
	// 	select {
	// 	case canCommit := <-prepareChan:
	// 		if canCommit {
	// 			canCommitCount++
	// 		}
	// 	case <-timeout:
	// 		fmt.Println("Timeout occurred during prepare phase")
	// 		// Send Abort to both shards
	// 		go senderClient.Abort(senderCtx, &pb.ClientAbort{TransactionId: txn.Id})
	// 		go receiverClient.Abort(receiverCtx, &pb.ClientAbort{TransactionId: txn.Id})
	// 		return
	// 	}
	// }

	// // Decide to Commit or Abort
	// if canCommitCount == 2 {
	// 	fmt.Println("Both shards agreed, sending Commit")
	// 	// Send Commit to both shards
	// 	go senderClient.Commit(senderCtx, &pb.ClientCommit{TransactionId: txn.Id})
	// 	go receiverClient.Commit(receiverCtx, &pb.ClientCommit{TransactionId: txn.Id})
	// } else {
	// 	fmt.Println("One or both shards did not agree, sending Abort")
	// 	// Send Abort to both shards
	// 	go senderClient.Abort(senderCtx, &pb.ClientAbort{TransactionId: txn.Id})
	// 	go receiverClient.Abort(receiverCtx, &pb.ClientAbort{TransactionId: txn.Id})
	// }
}

func sendIntraShardTransaction(txn *pb.Transaction, clusterIDToLeaderServerID map[int]int) {
	// Get shard for the sender (since it's intra-shard, sender and receiver are in the same shard)
	shardID := dataItemToCluster[int(txn.Sender)]
	// Use the leader server ID for this shard
	leaderServerID := clusterIDToLeaderServerID[shardID]

	// Get the server info
	var leaderServer Server
	servers := clusterToServers[shardID]
	for _, server := range servers {
		if server.ServerID == leaderServerID {
			leaderServer = server
			break
		}
	}

	// Set up RPC connection

	go func() {
		client, ctx, conn := setUpClientServerRPC(leaderServer.ServerID)
		fmt.Println("Sending to Leader ", leaderServer.ServerID)
		defer conn.Close()
		fmt.Println("Sending IntraShard Transaction")
		resp, err := client.IntraShardTransaction(ctx, txn)
		if err != nil {
			log.Printf("Error sending intra-shard transaction: %v", err)
			return
		}

		if resp.Success {
			fmt.Printf("Intra-shard transaction %d committed successfully.\n", txn.Id)
		} else {
			fmt.Printf("Intra-shard transaction %d failed: %s\n", txn.Id, resp.Message)
		}
	}()
}
func parseLeaders(leadersStr string) []int {
	// Similar to parseLiveServers
	leadersStr = strings.Trim(leadersStr, "[]")
	parts := strings.Split(leadersStr, ",")
	var leaders []int
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "S") {
			leaderIDStr := strings.TrimPrefix(part, "S")
			leaderID, _ := strconv.Atoi(leaderIDStr)
			leaders = append(leaders, leaderID)
		} else {
			leaderID, _ := strconv.Atoi(part)
			leaders = append(leaders, leaderID)
		}
	}
	return leaders
}
