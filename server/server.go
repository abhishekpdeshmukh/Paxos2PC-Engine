package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var dataItemToCluster = make(map[int]int)
var clusterToServers = make(map[int][]ServerConfig)
var currentSetIndex int
var serverIDToClusterID = make(map[int]int)
var clusterIDs []int
var shardItemsList []string

type TransactionRequest struct {
	Transaction *pb.Transaction
	ResultChan  chan *pb.ClientTransactionResponse
}
type Server struct {
	pb.UnimplementedClientServerConnectionServer
	pb.UnimplementedPaxosServiceServer
	ServerID         int
	ClusterID        int
	Port             int
	IP               string
	ShardItems       string
	lockMap          map[int]bool
	lock             sync.Mutex
	db               *sql.DB
	isActive         bool
	servers          []int
	promisedBallot   int
	balance          map[int]int
	transactionQueue chan *TransactionRequest
	acceptedBallot   int
	// transactionLog   map[int32]*TransactionState
	writeAheadLog  map[int]*pb.WAL
	transactionLog []*pb.Transaction
	ballotNum      int
}

func main() {
	// Check if there are enough arguments
	if len(os.Args) < 9 {
		fmt.Println("Usage: go run . --clusterId <id> --serverId <id> --ip <ip> --port <port> --shardItems <items>")
		return
	}

	// Parse command-line arguments
	var clusterId, serverId, ip, port, shardItems, servers string
	var serverList []int
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--clusterId":
			clusterId = os.Args[i+1]
		case "--serverId":
			serverId = os.Args[i+1]
		case "--ip":
			ip = os.Args[i+1]
		case "--port":
			port = os.Args[i+1]
		case "--shardItems":
			shardItems = os.Args[i+1]
		case "--serverIds":
			servers = os.Args[i+1]
		}
	}

	// Convert numeric arguments if necessary
	clusterIDInt, err := strconv.Atoi(clusterId)
	if err != nil {
		fmt.Println("Invalid cluster ID:", err)
		return
	}
	serverIDInt, err := strconv.Atoi(serverId)
	if err != nil {
		fmt.Println("Invalid server ID:", err)
		return
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		fmt.Println("Invalid port number:", err)
		return
	}
	shardItemsList := strings.Split(shardItems, " ")
	x := strings.Split(servers, ",")
	for _, item := range x {
		fmt.Println("ITem")
		fmt.Println(item)
		server_id, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil {
			fmt.Println("Invalid serverIDs", item)
			continue
		}
		serverList = append(serverList, server_id)
	}
	tempMap := make(map[int]bool)
	temp2Map := make(map[int]int)
	for _, item := range shardItemsList {
		shardID, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil {
			fmt.Println("Invalid shard item:", item)
			continue
		}
		tempMap[shardID] = false
		temp2Map[shardID] = 10
	}
	// Fill the Server struct
	server := &Server{
		ServerID:         serverIDInt,
		ClusterID:        clusterIDInt,
		Port:             portInt,
		IP:               ip,
		ShardItems:       shardItems,
		lockMap:          tempMap,
		isActive:         true,
		transactionQueue: make(chan *TransactionRequest, 100), // Buffer size as needed
		// transactionLog:   make(map[int32]*TransactionState),
		transactionLog: []*pb.Transaction{},
		writeAheadLog:  make(map[int]*pb.WAL),
		balance:        temp2Map,
		servers:        serverList,
		ballotNum:      0,
		acceptedBallot: 0,
		promisedBallot: 0,
		db:             InitDB(serverIDInt),
	}
	// for _, item := range shardItemsList {
	// 	shardID, err := strconv.Atoi(strings.TrimSpace(item))
	// 	server.updateBalance(shardID)
	// }
	// Print the filled struct
	// fmt.Printf("Server Struct: %+v\n", server)
	go readConfig("../config.json")
	go StartTransactionProcessor(server)
	// printServer(server)
	go setUpServerServerReceiver(server)
	go setUpClientServerReceiver(server)
	time.Sleep(100 * time.Millisecond)
	// Infinite loop to keep the program running
	for {
	}
}

func (server *Server) Kill(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	server.lock.Lock()
	defer server.lock.Unlock()
	server.isActive = false
	fmt.Println("CONSIDER ME DEAD")
	return &emptypb.Empty{}, nil
}

func (server *Server) Revive(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	server.lock.Lock()
	defer server.lock.Unlock()
	server.isActive = true
	fmt.Println("I AM BACK ALIVE!!!!!")
	return &emptypb.Empty{}, nil
}

// Prepare handles the Prepare phase of Paxos
// func (s *Server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
// 	s.lock.Lock()
// 	defer s.lock.Unlock()
// 	if s.isActive {
// 		incomingBallot := req.Ballot.BallotNum
// 		if incomingBallot >= int32(s.ballotNum) {
// 			s.ballotNum = int(incomingBallot)
// 			// Update promised ballot number
// 			s.promisedBallot = int(incomingBallot)

// 			fmt.Printf("Server %d: Promised ballot %d\n", s.ServerID, s.promisedBallot)

//				return &pb.PromiseResponse{
//					ServerId: int32(s.ServerID),
//					Logsize:  int32(len(s.transactionLog)),
//				}, nil
//			} else {
//				fmt.Printf("Server %d: Rejecting lower ballot %d\n", s.ServerID, incomingBallot)
//				return nil, fmt.Errorf("ballot number too low")
//			}
//		}
//		// time.Sleep(5 * time.Second)
//		return nil, fmt.Errorf("i am dead")
//	}
func (s *Server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.isActive {
		return nil, fmt.Errorf("server %d is not active", s.ServerID)
	}

	incomingBallot := req.Ballot.BallotNum
	leaderLogSize := req.LogSize

	// Check if the incoming ballot is valid
	if incomingBallot >= int32(s.ballotNum) {
		// Update the promised ballot number
		s.ballotNum = int(incomingBallot)
		s.promisedBallot = int(incomingBallot)

		fmt.Printf("Server %d: Promised ballot %d\n", s.ServerID, s.promisedBallot)

		// Check if the server is lagging
		lagging := len(s.transactionLog) < int(leaderLogSize)
		var missingLogs []*pb.Transaction
		if len(s.transactionLog) > int(leaderLogSize) {
			missingLogs = s.transactionLog[leaderLogSize:]
		}
		fmt.Println(" AM i lagging ", lagging)
		// Construct the PromiseResponse
		return &pb.PromiseResponse{
			ServerId:     int32(s.ServerID),
			BallotNumber: &pb.Ballot{BallotNum: int32(s.promisedBallot)},
			AcceptNum:    &pb.Ballot{BallotNum: int32(s.acceptedBallot)},
			AcceptVal:    missingLogs, // Assumes s.acceptedValues is a slice of TransactionRequest
			Logsize:      int32(len(s.transactionLog)),
			Lag:          lagging,
		}, nil
	} else {
		fmt.Printf("Server %d: Rejecting lower ballot %d\n", s.ServerID, incomingBallot)
		return nil, fmt.Errorf("ballot number too low")
	}
}

// // Accept handles the Accept phase of Paxos
// func (s *Server) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {

// 	if s.isActive {
// 		s.lock.Lock()
// 		defer s.lock.Unlock()
// 		if s.isPossible(req.Transaction) {

// 			incomingBallot := req.Ballot.BallotNum
// 			if incomingBallot >= int32(s.promisedBallot) {
// 				s.ballotNum = int(incomingBallot)
// 				s.acceptedBallot = int(incomingBallot)

// 				// Update transaction log with missing logs if any
// 				if req.MissingLogIdx < int32(len(s.transactionLog)) {
// 					s.transactionLog = s.transactionLog[:req.MissingLogIdx]
// 				}
// 				s.transactionLog = append(s.transactionLog, req.MissingLogs...)
// 				s.transactionLog = append(s.transactionLog, req.Transaction)

// 				fmt.Printf("Server %d: Accepted ballot %d\n", s.ServerID, s.acceptedBallot)

//					return &pb.AcceptedResponse{
//						ServerId: int32(s.ServerID),
//						Success:  true,
//					}, nil
//				} else {
//					fmt.Printf("Server %d: Rejecting Accept with lower ballot %d\n", s.ServerID, incomingBallot)
//					return nil, fmt.Errorf("ballot number too low")
//				}
//			}
//		}
//		time.Sleep(5 * time.Second)
//		return nil, fmt.Errorf("i am dead")
//	}
func (s *Server) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {
	if !s.isActive {
		time.Sleep(5 * time.Second)
		return nil, fmt.Errorf("server %d is not active", s.ServerID)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the transaction is possible
	incomingBallot := req.Ballot.BallotNum
	if incomingBallot >= int32(s.promisedBallot) {
		// Update ballot numbers
		s.ballotNum = int(incomingBallot)
		s.acceptedBallot = int(incomingBallot)

		// Synchronize logs with missing entries
		fmt.Println("req.MissingLogIdx > int32(len(s.transactionLog))")
		fmt.Println(req.MissingLogIdx > int32(len(s.transactionLog)))
		fmt.Println(req.MissingLogIdx)
		fmt.Println(req.MissingLogs)
		if req.MissingLogIdx > int32(len(s.transactionLog)) {
			// Commit transactions to the database
			fmt.Println("Hello I was lagging tnx for synching me leader")
			s.transactionLog = append(s.transactionLog, req.MissingLogs...)
			fmt.Println(s.transactionLog)
			for _, txn := range req.MissingLogs {
				if dataItemToCluster[int(req.Transaction.Sender)] == s.ClusterID {
					s.balance[int(txn.Sender)] -= int(txn.Amount)
				}
				if dataItemToCluster[int(txn.Receiver)] == s.ClusterID {
					s.balance[int(txn.Receiver)] += int(txn.Amount)
				}
				err := s.commitTransactions(txn)
				if err != nil {
					return nil, fmt.Errorf("error committing transaction to DB: %v", err)
				}
				err = s.updateBalance(txn)
				if err != nil {
					return nil, fmt.Errorf("error Updating Balance to DB: %v", err)
				}
				fmt.Println("Hi Getting synced")
				fmt.Println(txn)
			}
		}
		if !s.isPossible(req.Transaction) {
			return nil, fmt.Errorf("transaction is not possible")
		} else {
			fmt.Printf("Server %d: Accepted ballot %d\n", s.ServerID, s.acceptedBallot)
			return &pb.AcceptedResponse{
				ServerId: int32(s.ServerID),
				Success:  true,
			}, nil
		}
	}
	fmt.Printf("Server %d: Rejecting Accept with lower ballot %d\n", s.ServerID, incomingBallot)
	return nil, fmt.Errorf("ballot number too low")
}

// Commit handles the Commit phase of Paxos
func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitedResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.isActive {
		// Apply the transaction to the state machine
		if !isCrossShard(req.Transaction) {
			s.transactionLog = append(s.transactionLog, req.Transaction)
			fmt.Printf("Server %d: Committed transaction\n", s.ServerID)
			s.balance[int(req.Transaction.Sender)] -= int(req.Transaction.Amount)
			s.balance[int(req.Transaction.Receiver)] += int(req.Transaction.Amount)
			s.commitTransactions(req.Transaction)
			s.updateBalance(req.Transaction)
			senderShard := int(req.Transaction.Sender)
			receiverShard := int(req.Transaction.Receiver)
			s.LockOperation(false, senderShard, receiverShard)
		} else {
			var temp int
			var temp2 int
			var temp3 int32
			if dataItemToCluster[int(req.Transaction.Sender)] == s.ClusterID {
				temp = s.balance[int(req.Transaction.Sender)]
				s.balance[int(req.Transaction.Sender)] -= int(req.Transaction.Amount)
				temp2 = s.balance[int(req.Transaction.Sender)]
				temp3 = req.Transaction.Sender
			} else {
				temp = s.balance[int(req.Transaction.Receiver)]
				s.balance[int(req.Transaction.Receiver)] += int(req.Transaction.Amount)
				temp2 = s.balance[int(req.Transaction.Receiver)]
				temp3 = req.Transaction.Receiver
			}
			s.writeAheadLog[int(req.Transaction.Id)] = &pb.WAL{
				PreviousBalance: int32(temp),
				LatestBalance:   int32(temp2),
				Shard:           temp3,
			}

		}
		return &pb.CommitedResponse{
			// ServerId: int32(s.ServerID),
			Success: true,
		}, nil
	}
	time.Sleep(5 * time.Second)
	return nil, fmt.Errorf("i am dead")
}

// Implement IntraShardTransaction RPC
func (s *Server) IntraShardTransaction(ctx context.Context, req *pb.Transaction) (*pb.ClientTransactionResponse, error) {
	// Create a result channel

	resultChan := make(chan *pb.ClientTransactionResponse)

	// Create the TransactionRequest
	txnReq := &TransactionRequest{
		Transaction: req,
		ResultChan:  resultChan,
	}

	// Enqueue the transaction request
	fmt.Println("Server ", s.ServerID, " reporting transaction\n", txnReq)
	s.lock.Lock()
	s.transactionQueue <- txnReq
	s.lock.Unlock()
	// Wait for the result
	response := <-resultChan

	// Return the response to the client
	return response, nil
}

func (s *Server) CrossShardTransaction(req *pb.Transaction) *pb.ClientTransactionResponse {
	// Create a result channel

	resultChan := make(chan *pb.ClientTransactionResponse)
	fmt.Println("Inside CrossShardTransaction")
	// Create the TransactionRequest
	txnReq := &TransactionRequest{
		Transaction: req,
		ResultChan:  resultChan,
	}

	// Enqueue the transaction request
	fmt.Println("Cross Shard Server ", s.ServerID, " reporting transaction\n", txnReq)
	s.lock.Lock()
	s.transactionQueue <- txnReq
	s.lock.Unlock()
	// Wait for the result
	fmt.Println("Cross Shard Transaction Added to the Queue")
	response := <-resultChan

	// Return the response to the client
	fmt.Println("Recieved Response from servers ", response)
	return response
}

// Implement other methods as needed...
func (s *Server) processTransaction(txn TransactionRequest) (bool, string) {
	// Implement your Paxos consensus algorithm here
	// For example, initiate a Paxos instance and wait for consensus

	// Simulate transaction processing
	fmt.Printf("Server %d processing transaction %d\n", s.ServerID, txn.Transaction.Id)
	_, exists1 := s.balance[int(txn.Transaction.Sender)]
	_, exists2 := s.balance[int(txn.Transaction.Receiver)]
	flag := false
	if exists1 && exists2 {
		// Key exists in the map
		// fmt.Println("Key exists with value:", value1)
		fmt.Println("This is clearly intra shard need to process it with paxos")
		flag = s.BroadCastPrepare(txn)
	} else {
		// Key does not exist in the map
		fmt.Println("This is a cross shard transaction")
		flag = s.BroadCastPrepare(txn)
	}
	if flag {
		return true, "Transaction committed successfully"
	}
	return false, "Aborted this transaction"
}

func StartTransactionProcessor(s *Server) {
	fmt.Println("Inside start transaction processor")
	for txnReq := range s.transactionQueue {
		// Process the transaction
		fmt.Println("Starting the paxos for ", txnReq.Transaction.Id)
		success, message := s.processTransaction(*txnReq)

		// Send the result back
		txnReq.ResultChan <- &pb.ClientTransactionResponse{
			Success: success,
			Message: message,
		}
	}
}

// GetTransactionsRPC handles the GetTransactions RPC call
func (s *Server) GetTransactions(ctx context.Context, req *emptypb.Empty) (*pb.GetTransactionsResponse, error) {
	transactions, err := s.GetTransaction()
	if err != nil {
		return nil, err
	}

	return &pb.GetTransactionsResponse{
		Transactions: transactions,
	}, nil
}

// GetBalancesRPC handles the GetBalances RPC call
func (s *Server) GetBalances(ctx context.Context, req *pb.GetBalancesRequest) (*pb.GetBalancesResponse, error) {
	if s.checkShard(int(req.ShardId)) {
		balances, err := s.GetBalance(int(req.ShardId))
		fmt.Println("Error is ", err)
		fmt.Println(balances.ShardId, " : ", balances.Balance)
		if err != nil {
			return nil, err
		}
		return &pb.GetBalancesResponse{
			Balances: balances,
		}, nil
	}
	return &pb.GetBalancesResponse{
		Balances: nil,
	}, nil

}

// func (s *Server) GetBalances(ctx context.Context, req *pb.GetBalancesRequest) (*pb.GetBalancesResponse, error) {
// 	// Check if the shard exists in the balance map
// 	shardID := int(req.ShardId)
// 	flag := false
// 	if _, exists := s.balance[shardID]; !exists {
// 		return &pb.GetBalancesResponse{
// 			Balances: nil,
// 		}, nil
// 	} else {
// 		// If the shard exists, call the GetBalance function
// 		balances, err := s.GetBalance(shardID)
// 		if !flag && err != nil {
// 			fmt.Println("Error is ", err)
// 			return nil, err
// 		}
// 		fmt.Println(balances.ShardId, " : ", balances.Balance)
// 		return &pb.GetBalancesResponse{
// 			Balances: balances,
// 		}, nil
// 	}
// }

func (s *Server) ClearDB(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	DeleteDB(s.ServerID)
	// s.balance = make(map[int]int)
	for _, item := range shardItemsList {
		shardID, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil {
			fmt.Println("Invalid shard item:", item)
			continue
		}

		s.balance[shardID] = 10
	}
	return &emptypb.Empty{}, nil
}
