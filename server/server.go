package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

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
		transactionLog: make([]*pb.Transaction, 100),
		balance:        temp2Map,
		servers:        serverList,
		ballotNum:      0,
		acceptedBallot: 0,
		promisedBallot: 0,
		db:             InitDB(serverIDInt),
	}

	// Print the filled struct
	// fmt.Printf("Server Struct: %+v\n", server)
	go StartTransactionProcessor(server)
	// printServer(server)
	go setUpServerServerReceiver(server)
	go setUpClientServerReceiver(server)
	fmt.Println("HIIIIIIIII")
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
func (s *Server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	incomingBallot := req.Ballot.BallotNum
	if incomingBallot > int32(s.ballotNum) {
		s.ballotNum = int(incomingBallot)
		// Update promised ballot number
		s.promisedBallot = int(incomingBallot)

		fmt.Printf("Server %d: Promised ballot %d\n", s.ServerID, s.promisedBallot)

		return &pb.PromiseResponse{
			ServerId: int32(s.ServerID),
			Logsize:  int32(len(s.transactionLog)),
		}, nil
	} else {
		fmt.Printf("Server %d: Rejecting lower ballot %d\n", s.ServerID, incomingBallot)
		return nil, fmt.Errorf("ballot number too low")
	}
}

// Accept handles the Accept phase of Paxos
func (s *Server) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	incomingBallot := req.Ballot.BallotNum
	if incomingBallot >= int32(s.promisedBallot) {
		s.ballotNum = int(incomingBallot)
		s.acceptedBallot = int(incomingBallot)

		// Update transaction log with missing logs if any
		if req.MissingLogIdx < int32(len(s.transactionLog)) {
			s.transactionLog = s.transactionLog[:req.MissingLogIdx]
		}
		s.transactionLog = append(s.transactionLog, req.MissingLogs...)
		s.transactionLog = append(s.transactionLog, req.Transaction)

		fmt.Printf("Server %d: Accepted ballot %d\n", s.ServerID, s.acceptedBallot)

		return &pb.AcceptedResponse{
			ServerId: int32(s.ServerID),
		}, nil
	} else {
		fmt.Printf("Server %d: Rejecting Accept with lower ballot %d\n", s.ServerID, incomingBallot)
		return nil, fmt.Errorf("ballot number too low")
	}
}

// Commit handles the Commit phase of Paxos
func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitedResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Apply the transaction to the state machine
	s.transactionLog = append(s.transactionLog, req.Transaction)

	fmt.Printf("Server %d: Committed transaction\n", s.ServerID)
	s.balance[int(req.Transaction.Sender)] -= int(req.Transaction.Amount)
	s.balance[int(req.Transaction.Receiver)] += int(req.Transaction.Amount)
	s.commitTransactions(req.Transaction)
	s.updateBalance(req.Transaction)
	return &pb.CommitedResponse{
		// ServerId: int32(s.ServerID),
		Success: true,
	}, nil
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

// Implement other methods as needed...
func (s *Server) processTransaction(txn TransactionRequest) (bool, string) {
	// Implement your Paxos consensus algorithm here
	// For example, initiate a Paxos instance and wait for consensus

	// Simulate transaction processing
	fmt.Printf("Server %d processing transaction %d\n", s.ServerID, txn.Transaction.Id)
	_, exists1 := s.balance[int(txn.Transaction.Sender)]
	_, exists2 := s.balance[int(txn.Transaction.Receiver)]
	if exists1 && exists2 {
		// Key exists in the map
		// fmt.Println("Key exists with value:", value1)
		fmt.Println("This is clearly intra shard need to process it with paxos")
		s.BroadCastPrepare(txn)
	} else {
		// Key does not exist in the map
		fmt.Println("Cross shard need 2pc")
	}
	return true, "Transaction committed successfully"
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
	balances, err := s.GetBalance(int(req.ShardId))
	if err != nil {
		return nil, err
	}

	return &pb.GetBalancesResponse{
		Balances: balances,
	}, nil
}
