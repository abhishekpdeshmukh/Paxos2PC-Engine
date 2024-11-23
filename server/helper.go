package main

import (
	"fmt"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
)

func (s *Server) BroadCastPrepare(txn *pb.TransactionRequest) {
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
			}
			if receivedPromises >= majority {
				break CollectPromises
			}
		case err := <-errorCh:
			fmt.Println("Prepare error:", err)
		case <-timeout:
			fmt.Println("Prepare phase timed out. Sending abort.")
			// s.sendAbort()
			return
		}
	}
	fmt.Println("Received majority promises.")

	// Accept phase
	acceptCh := make(chan *pb.AcceptedResponse, totalServers-1)
	errorCh = make(chan error, totalServers-1) // Reuse errorCh
	receivedAccepts := 1                       // Counting self

	// Send Accept messages
	for _, serverID := range s.servers {
		if serverID != s.ServerID {
			// Determine missing logs for the server
			var missingLogs []*pb.TransactionRequest
			if logSize, ok := laggingServers[serverID]; ok {
				missingLogs = s.transactionLog[logSize:]
			} else {
				missingLogs = []*pb.TransactionRequest{}
			}

			go func(n int, logs []*pb.TransactionRequest) {
				c, ctx, conn := setUpServerServerSender(n)
				defer conn.Close()

				accept, err := c.Accept(ctx, &pb.AcceptRequest{
					Ballot: &pb.Ballot{
						BallotNum: int32(s.ballotNum),
						NodeID:    int32(s.ServerID),
					},
					Transactionreq: txn,
					MissingLogs:    logs,
					MissingLogIdx:  int32(len(s.transactionLog)),
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
			fmt.Println("Accept phase timed out. Sending abort.")
			// s.sendAbort()
			return
		}
	}
	fmt.Println("Received majority accepts.")

	// Commit phase
	// Send Commit messages
	for _, serverID := range s.servers {
		if serverID != s.ServerID {
			go func(n int) {
				c, ctx, conn := setUpServerServerSender(n)
				defer conn.Close()

				_, err := c.Commit(ctx, &pb.CommitRequest{
					Transactionreq: txn,
				})
				if err != nil {
					fmt.Printf("Commit error on Server %d: %v\n", n, err)
				} else {
					fmt.Printf("Committed on Server %d\n", n)
				}
			}(serverID)
		}
	}
	fmt.Println("Transaction committed successfully.")
}

// sendAbort sends abort messages to all servers
// func (s *Server) sendAbort() {
//     for _, serverID := range s.servers {
//         if serverID != s.ServerID {
//             go func(n int) {
//                 c, ctx, conn := setUpServerServerSender(n)
//                 defer conn.Close()
//                 _, err := c.Abort(ctx, &pb.AbortRequest{
//                     NodeID: int32(s.ServerID),
//                 })
//                 if err != nil {
//                     fmt.Printf("Abort error on Server %d: %v\n", n, err)
//                 } else {
//                     fmt.Printf("Abort sent to Server %d\n", n)
//                 }
//             }(serverID)
//         }
//     }
// }
