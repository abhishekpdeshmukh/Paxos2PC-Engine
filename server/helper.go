package main

import (
	"fmt"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
)

func (s *Server) BroadCastPrepare(txn TransactionRequest) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.ballotNum++
	fmt.Println("Starting Paxos with Ballot Number", s.ballotNum)

	totalServers := len(s.servers)
	majority := (totalServers / 2) + 1

	// Channels to collect promises and errors
	promiseCh := make(chan *pb.PromiseResponse, totalServers-1)
	errorCh := make(chan error, totalServers-1)
	laggingServers := make(map[int]int32) // ServerID -> LogSize

	// Send Prepare messages

	for _, serverID := range s.servers {
		if serverID != s.ServerID {
			go func(n int) {
				c, ctx, conn := setUpServerServerSender(n)
				defer conn.Close()

				// Prepare request
				promise, err := c.Prepare(ctx, &pb.PrepareRequest{
					Ballot: &pb.Ballot{
						BallotNum: int32(s.ballotNum),
						NodeID:    int32(s.ServerID),
					},
					LogSize: int32(len(s.transactionLog)),
				})
				if err != nil {
					errorCh <- err
				} else {
					promiseCh <- promise
				}
			}(serverID)
		}
	}
	// Variables to track the latest logs
	var tempLogs []*pb.Transaction
	highestLogSize := len(s.transactionLog)
	// Collect promises with timeout
	receivedPromises := 1 // Counting self
	timeout := time.After(5 * time.Second)
CollectPromises:
	for {
		select {
		case promise := <-promiseCh:
			receivedPromises++
			fmt.Printf("Received promise from Server %d\n", promise.ServerId)
			if promise.Logsize < int32(len(s.transactionLog)) {
				laggingServers[int(promise.ServerId)] = promise.Logsize
				fmt.Println(" Lagging server map logsize ", promise.Logsize)
				fmt.Println(laggingServers)
			}
			fmt.Println("if promise.Lag && int(promise.Logsize) > highestLogSize {")
			fmt.Println(int(promise.Logsize) > highestLogSize)
			fmt.Println(promise)
			if int(promise.Logsize) > highestLogSize {
				highestLogSize = int(promise.Logsize)
				tempLogs = promise.AcceptVal
				fmt.Println("Inside AcceptVAL")
				fmt.Println(tempLogs)
			}
			if receivedPromises >= majority {
				break CollectPromises
			}
		case err := <-errorCh:
			fmt.Println("Prepare error:", err)
		case <-timeout:
			fmt.Println("Prepare phase timed out. Sending abort.")
			// s.sendAbort()
			return false
		}
	}
	fmt.Println("Received majority promises.")
	if len(tempLogs) > 0 {
		fmt.Printf("Synchronizing logs with latest log size: %d\n", highestLogSize)
		for _, txn := range tempLogs {
			// Commit each transaction to the database
			if dataItemToCluster[int(txn.Sender)] == s.ClusterID {
				s.balance[int(txn.Sender)] -= int(txn.Amount)
			}
			if dataItemToCluster[int(txn.Receiver)] == s.ClusterID {
				s.balance[int(txn.Receiver)] += int(txn.Amount)
			}
			err := s.commitTransactions(txn)
			if err != nil {
				fmt.Errorf("error committing transaction during synchronization: %v", err)
			}
			err = s.updateBalance(txn)
			if err != nil {
				fmt.Errorf("error updating balance during synchronization: %v", err)
			}
			// Update the transaction log
			fmt.Println("Getting synced")
			fmt.Println(txn)
			s.transactionLog = append(s.transactionLog, txn)

		}
	}
	// Accept phase
	acceptCh := make(chan *pb.AcceptedResponse, totalServers-1)
	errorCh = make(chan error, totalServers-1) // Reuse errorCh
	receivedAccepts := 1                       // Counting self
	if s.isPossible(txn.Transaction) {
		// Send Accept messages
		for _, serverID := range s.servers {
			if serverID != s.ServerID {
				// Determine missing logs for the server
				var missingLogs []*pb.Transaction
				fmt.Println("Server ID sending Accept to ", serverID)
				fmt.Println("With log size ", laggingServers[serverID])
				if logSize, ok := laggingServers[serverID]; ok {
					missingLogs = s.transactionLog[logSize:]
					fmt.Println("Missing logs")
					fmt.Println(missingLogs)
				} else {
					fmt.Println("In here missing section but empty")
					missingLogs = []*pb.Transaction{}
				}

				go func(n int, logs []*pb.Transaction) {
					c, ctx, conn := setUpServerServerSender(n)
					defer conn.Close()

					accept, err := c.Accept(ctx, &pb.AcceptRequest{
						Ballot: &pb.Ballot{
							BallotNum: int32(s.ballotNum),
							NodeID:    int32(s.ServerID),
						},
						Transaction:   txn.Transaction,
						MissingLogs:   logs,
						MissingLogIdx: int32(len(s.transactionLog)),
					})
					if err != nil {
						errorCh <- err
					} else {
						acceptCh <- accept
					}
				}(serverID, missingLogs)
			}
		}

		// Collect accepts with timeout
		timeout = time.After(5 * time.Second)
	CollectAccepts:
		for {
			select {
			case accept := <-acceptCh:
				receivedAccepts++
				fmt.Printf("Received accept from Server %d\n", accept.ServerId)
				if receivedAccepts >= majority {
					break CollectAccepts
				}
			case err := <-errorCh:
				fmt.Println("Accept error:", err)
			case <-timeout:
				fmt.Println("Paxos Accept phase timed out. Sending abort.")
				// s.sendAbort()
				return false
			}
		}
		fmt.Println("Received Paxos majority accepts.")

		// Commit phase
		// Send Commit messages
		for _, serverID := range s.servers {
			if serverID != s.ServerID {
				go func(n int) {
					c, ctx, conn := setUpServerServerSender(n)
					defer conn.Close()

					_, err := c.Commit(ctx, &pb.CommitRequest{
						Transaction: txn.Transaction,
					})
					if err != nil {
						fmt.Printf("Commit error on Server %d: %v\n", n, err)
					} else {
						fmt.Printf("Committed on Server %d\n", n)
					}
				}(serverID)
			}
		}
		s.transactionLog = append(s.transactionLog, txn.Transaction)
		fmt.Println("Appending to leader")
		fmt.Println(s.transactionLog)
		if !isCrossShard(txn.Transaction) {
			s.balance[int(txn.Transaction.Sender)] -= int(txn.Transaction.Amount)
			s.balance[int(txn.Transaction.Receiver)] += int(txn.Transaction.Amount)
			s.commitTransactions(txn.Transaction)
			err := s.updateBalance(txn.Transaction)
			if err != nil {
				fmt.Println(err)
			}
			s.LockOperation(false, int(txn.Transaction.Sender), int(txn.Transaction.Receiver))
		} else {
			var temp int
			var temp2 int
			var temp3 int32
			if dataItemToCluster[int(txn.Transaction.Sender)] == s.ClusterID {
				temp = s.balance[int(txn.Transaction.Sender)]
				s.balance[int(txn.Transaction.Sender)] -= int(txn.Transaction.Amount)
				temp2 = s.balance[int(txn.Transaction.Sender)]
				temp3 = txn.Transaction.Sender
			} else {
				temp = s.balance[int(txn.Transaction.Receiver)]
				s.balance[int(txn.Transaction.Receiver)] += int(txn.Transaction.Amount)
				temp2 = s.balance[int(txn.Transaction.Receiver)]
				temp3 = txn.Transaction.Receiver
			}
			s.writeAheadLog[int(txn.Transaction.Id)] = &pb.WAL{
				PreviousBalance: int32(temp),
				LatestBalance:   int32(temp2),
				Shard:           temp3,
			}
			fmt.Println("For ID ", s.writeAheadLog[int(txn.Transaction.Id)])
			fmt.Println(s.writeAheadLog[int(txn.Transaction.Id)])
		}
		fmt.Println("Paxos Transaction committed successfully.")
		return true
	}
	fmt.Println("Paxos Transaction ABORTED.")
	return false
}

func (s *Server) isPossible(txn *pb.Transaction) bool {

	senderShard := int(txn.Sender)
	receiverShard := int(txn.Receiver)
	// Check sender's balance only for intra-shard transactions
	if (dataItemToCluster[senderShard] == s.ClusterID) && s.balance[senderShard] < int(txn.Amount) {
		fmt.Printf("Transaction not possible: insufficient balance in Shard %d.\n", senderShard)
		return false
	}
	t1 := dataItemToCluster[senderShard] == s.ClusterID && s.lockMap[senderShard]
	t2 := dataItemToCluster[receiverShard] == s.ClusterID && s.lockMap[receiverShard]

	// Check if locks are available
	if t1 || t2 {
		fmt.Printf("Transaction not possible: lock already held on Shard %d or Shard %d.\n", senderShard, receiverShard)
		return false
	}

	// Acquire locks
	s.LockOperation(true, senderShard, receiverShard)

	return true
}

func (s *Server) LockOperation(flag bool, senderShard int, receiverShard int) {
	if dataItemToCluster[senderShard] == s.ClusterID {
		if flag {
			s.lockMap[senderShard] = true
			fmt.Println("SHARD ", senderShard, " IN CLUSTER ", s.ClusterID)
			fmt.Printf("Locks acquired for Shards %d ", senderShard)
		} else {
			s.lockMap[senderShard] = false
			fmt.Printf("Locks released for Shards %d ", senderShard)
		}

	}
	if dataItemToCluster[receiverShard] == s.ClusterID {
		if flag {
			s.lockMap[receiverShard] = true
			fmt.Println("SHARD ", receiverShard, " IN CLUSTER ", s.ClusterID)
			fmt.Printf("Locks acquired for Shards %d ", receiverShard)
		} else {
			s.lockMap[receiverShard] = false
			fmt.Printf("Locks released for Shards %d ", receiverShard)
		}

	}
}
