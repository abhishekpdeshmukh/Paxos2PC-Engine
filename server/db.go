package main

import (
	"database/sql"
	"fmt"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	_ "github.com/mattn/go-sqlite3"
)

func (s *Server) commitTransactions(transaction *pb.Transaction) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT INTO transactions (id, from_account, to_account, amount, status)
                             VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	fmt.Println("Trying For ", transaction.Id, transaction.Sender, transaction.Receiver, transaction.Amount, "Committed")
	_, err = stmt.Exec(transaction.Id, transaction.Sender, transaction.Receiver, transaction.Amount, "Committed")
	if err != nil {
		tx.Rollback()
		fmt.Println(err)
		fmt.Println("Error Executing the transaction")
		return err
	}
	fmt.Println("Transaction Persisten in DB")
	return tx.Commit()
}

func (s *Server) updateBalance(transaction *pb.Transaction) error {
	fmt.Println("Inside Update Balance")

	// SQL UPSERT query: Insert a new row or update an existing row
	query := `
        INSERT INTO balance (shard_id, balance)
        VALUES (?, ?)
        ON CONFLICT(shard_id) DO UPDATE SET balance = excluded.balance;
    `

	// Update sender balance
	if dataItemToCluster[int(transaction.Sender)] == s.ClusterID {
		senderBalance := s.balance[int(transaction.Sender)]
		_, err := s.db.Exec(query, transaction.Sender, senderBalance)
		if err != nil {
			return fmt.Errorf("error updating or inserting balance for sender shard_id %d: %v", transaction.Sender, err)
		}
		fmt.Printf("Successfully updated or inserted sender shard_id %d with new balance %d\n", transaction.Sender, senderBalance)

	}
	if dataItemToCluster[int(transaction.Receiver)] == s.ClusterID {
		// Update receiver balance
		receiverBalance := s.balance[int(transaction.Receiver)]
		_, err := s.db.Exec(query, transaction.Receiver, receiverBalance)
		if err != nil {
			return fmt.Errorf("error updating or inserting balance for receiver shard_id %d: %v", transaction.Receiver, err)
		}
		fmt.Printf("Successfully updated or inserted receiver shard_id %d with new balance %d\n", transaction.Receiver, receiverBalance)

	}
	return nil
}

// GetTransactions reads committed transactions from the database
func (s *Server) GetTransaction() ([]*pb.TransactionLog, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Query to select committed transactions
	query := `SELECT id, from_account, to_account, amount, status FROM transactions WHERE status = 'Committed'`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying transactions: %v", err)
	}
	defer rows.Close()

	var transactions []*pb.TransactionLog

	for rows.Next() {
		var id int64
		var fromAccount int64
		var toAccount int64
		var amount float64
		var status string

		// Scan the row into variables
		err := rows.Scan(&id, &fromAccount, &toAccount, &amount, &status)
		if err != nil {
			return nil, fmt.Errorf("error scanning transaction row: %v", err)
		}

		// Create a Transaction object
		transaction := &pb.TransactionLog{
			Id:       int32(id),
			Sender:   int32(fromAccount),
			Receiver: int32(toAccount),
			Amount:   int32(amount),
			Status:   status,
		}
		transactions = append(transactions, transaction)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transaction rows: %v", err)
	}

	return transactions, nil
}

// GetBalances reads the balances from the database
func (s *Server) GetBalance(shardID int) (*pb.BalanceEntry, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fmt.Println("Inside Getbalance")
	// Query to select the balance for the specified shard_id
	query := `SELECT shard_id, balance FROM balance WHERE shard_id = ?`
	fmt.Println("Post Query")
	row := s.db.QueryRow(query, shardID)

	var id int
	var balance int

	// Scan the result into variables
	err := row.Scan(&id, &balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no balance found for shard_id %d", shardID)
		}
		return nil, fmt.Errorf("error querying balance for shard_id %d: %v", shardID, err)
	}

	// Create and return the BalanceEntry object
	balanceEntry := &pb.BalanceEntry{
		ShardId: int64(id),
		Balance: int64(balance),
	}
	fmt.Println(balanceEntry)
	return balanceEntry, nil
}

func (s *Server) checkShard(shardID int) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	fmt.Println("Checking existence of shard:", shardID)

	// Query to check if the shard_id exists in the balance table
	query := `SELECT EXISTS(SELECT 1 FROM balance WHERE shard_id = ?)`

	var exists bool

	// Execute the query and scan the result into the `exists` variable
	err := s.db.QueryRow(query, shardID).Scan(&exists)
	if err != nil {
		fmt.Println(err)
	}

	return exists
}

func DeleteDB(serverIDInt int) {
	// Open the SQLite database connection
	db, err := sql.Open("sqlite3", fmt.Sprintf("node_%d.db", serverIDInt))
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	defer db.Close()

	// Begin a transaction to ensure all deletions are atomic
	tx, err := db.Begin()
	if err != nil {
		fmt.Println("Error starting transaction:", err)
		return
	}

	// Clear all records from the transactions table
	_, err = tx.Exec("DELETE FROM transactions")
	if err != nil {
		fmt.Println("Error clearing transactions table:", err)
		tx.Rollback()
		return
	}

	// Clear all records from the balance table
	_, err = tx.Exec("DELETE FROM balance")
	if err != nil {
		fmt.Println("Error clearing balance table:", err)
		tx.Rollback()
		return
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		fmt.Println("Error committing transaction:", err)
		return
	}

	fmt.Println("Database records cleared successfully!")
}
