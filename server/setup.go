package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	_ "github.com/mattn/go-sqlite3"
)

func InitDB(serverIDInt int) *sql.DB {
	// Open a new SQLite database connection
	db, err := sql.Open("sqlite3", fmt.Sprintf("node_%d.db", serverIDInt))
	if err != nil {
		fmt.Println("Error opening database:", err)
		return nil
	}

	// Create a transactions table if it doesn't exist
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY,
        from_account INTEGER,
        to_account INTEGER,
        amount INTEGER,
		status STRING
    )`)
	if err != nil {
		fmt.Println("Error creating transactions table:", err)
	}

	// Create a shard_balances table if it doesn't exist
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS balance (
        shard_id INTEGER PRIMARY KEY,
        balance INTEGER
    )`)
	if err != nil {
		fmt.Println("Error creating shard_balances table:", err)
	}
	return db
}

type TransactionState struct {
	txn      *pb.Transaction
	prepared bool
}

func printServer(server *Server) {
	// Formatted message to describe the server properties
	fmt.Printf("\n--- Server Properties Upon Spawning ---\n")
	fmt.Printf("Server ID   : %d\n", server.ServerID)
	fmt.Printf("Cluster ID  : %d\n", server.ClusterID)
	fmt.Printf("Port        : %d\n", server.Port)
	fmt.Printf("IP Address  : %s\n", server.IP)
	fmt.Printf("Shard Items : %s\n", server.ShardItems)
	fmt.Printf("Is Active   : %t\n", server.isActive)
	fmt.Println("Lock Map    :")
	// for shard, locked := range server.lockMap {
	// 	fmt.Printf("  - Shard %d: Locked = %t\n", shard, locked)
	// }
	// for shard, balance := range server.balance {
	// 	fmt.Printf("  - Shard %d: Balance = %d\n", shard, balance)
	// }
	fmt.Println(server.servers)
	for _, serverid := range server.servers {
		fmt.Printf("Server ID on this Clusters ", serverid)
	}
	fmt.Println("---------------------------------------\n")
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
		clusterIDs = append(clusterIDs, clusterID)

		// Map server IDs to cluster IDs
		for _, server := range cluster.Servers {
			serverIDToClusterID[server.ServerID] = clusterID
		}

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
	// fmt.Println(dataItemToCluster)
}

func isCrossShard(transaction *pb.Transaction) bool {
	return dataItemToCluster[int(transaction.Sender)] != dataItemToCluster[int(transaction.Receiver)]
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
