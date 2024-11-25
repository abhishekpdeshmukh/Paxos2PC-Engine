package main

import (
	"database/sql"
	"fmt"
	_ "modernc.org/sqlite"
	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
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
	_, err = stmt.Exec(transaction.Id, transaction.Sender, transaction.Receiver, transaction.Amount, "Committed")
	if err != nil {
		tx.Rollback()
		fmt.Println("Error Executing the transaction")
		return err
	}
	fmt.Println("Transaction Persisten in DB")
	return tx.Commit()
}

func (s *Server) updateBalance(transaction *pb.Transaction) error {
	// Prepare the SQL statement
	query := `UPDATE balance SET balance = ? WHERE shard_id = ?`
	senderBalance := s.balance[int(transaction.Sender)]
	receiverBalance := s.balance[int(transaction.Receiver)]
	// Execute the statement
	result, err := s.db.Exec(query, senderBalance, transaction.Sender)
	if err != nil {
		return fmt.Errorf("error updating balance for shard_id %d: %v", transaction.Sender, err)
	}

	// Check if any rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking rows affected: %v", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("no rows updated for shard_id %d", transaction.Sender)
	}

	fmt.Printf("Successfully updated shard_id %d with new balance %d\n", transaction.Sender, senderBalance)

	result, err = s.db.Exec(query, receiverBalance, transaction.Receiver)
	if err != nil {
		return fmt.Errorf("error updating balance for shard_id %d: %v", transaction.Receiver, err)
	}

	// Check if any rows were affected
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking rows affected: %v", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("no rows updated for shard_id %d", transaction.Receiver)
	}

	fmt.Printf("Successfully updated shard_id %d with new balance %d\n", transaction.Receiver, receiverBalance)
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

	// Query to select the balance for the specified shard_id
	query := `SELECT shard_id, balance FROM balance WHERE shard_id = ?`

	row := s.db.QueryRow(query, shardID)

	var id int64
	var balance int64

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
		ShardId: id,
		Balance: balance,
	}

	return balanceEntry, nil
}
