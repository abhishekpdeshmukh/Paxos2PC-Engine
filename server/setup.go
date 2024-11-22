package main

import (
	"database/sql"
	"fmt"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
)

func (s *Server) InitDB() {
	// Open a new SQLite database connection
	db, err := sql.Open("sqlite3", fmt.Sprintf("node_%d.db", s.ServerID))
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	s.db = db

	// Create a transactions table if it doesn't exist
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY,
        set_number INTEGER,
        from_account INTEGER,
        to_account INTEGER,
        amount INTEGER
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
	for shard, locked := range server.lockMap {
		fmt.Printf("  - Shard %d: Locked = %t\n", shard, locked)
	}
	for shard, balance := range server.balance {
		fmt.Printf("  - Shard %d: Balance = %d\n", shard, balance)
	}
	fmt.Println("---------------------------------------\n")
}
