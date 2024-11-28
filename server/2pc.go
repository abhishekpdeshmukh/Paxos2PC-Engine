package main

import (
	"context"
	"fmt"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *Server) TwoPCPrepare(ctx context.Context, req *pb.ClientPrepare) (*pb.ClientPrepareResponse, error) {
	s.lock.Lock()

	// Check if the server is active
	if !s.isActive {
		return nil, fmt.Errorf("server is inactive")
	}

	resultChan := make(chan bool)
	fmt.Println("Hello Inside TWOPC prepare")
	s.lock.Unlock()
	go func() {
		success := s.CrossShardTransaction(req.Transaction)
		resultChan <- success.Success
	}()
	fmt.Println("CrossShard Sent sucessfully")
	success := <-resultChan
	s.lock.Lock()
	defer s.lock.Unlock()
	fmt.Println("Result Arrived ", success)
	if success {
		// Write to WAL
		// s.writeAheadLog(txn.Id)= [txn]
		return &pb.ClientPrepareResponse{CanCommit: true}, nil
	} else {
		return &pb.ClientPrepareResponse{CanCommit: false}, nil
	}
}

func (s *Server) TwoPCCommit(ctx context.Context, req *pb.ClientCommit) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the server is active
	if !s.isActive {
		return nil, fmt.Errorf("server is inactive")
	}
	s.updateBalance(req.Transaction)
	s.commitTransactions(req.Transaction)
	// s.writeAheadLog(txn.Id)= [txn]
	senderShard := int(req.Transaction.Sender)
	receiverShard := int(req.Transaction.Receiver)
	s.LockOperation(false, senderShard, receiverShard)
	return &emptypb.Empty{}, nil
}

func (s *Server) TwoPCAbort(ctx context.Context, req *pb.ClientAbort) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the server is active
	if !s.isActive {
		return nil, fmt.Errorf("server is inactive")
	}
	fmt.Println("Aborting the Transaction Cool")

	walEntry, exists := s.writeAheadLog[int(req.Transaction.Id)]
	if !exists {
		fmt.Printf("Transaction ID %d does not exist in writeAheadLog\n", req.Transaction.Id)
				
		return &emptypb.Empty{}, nil
	}
	if _, shardExists := s.balance[int(walEntry.Shard)]; !shardExists {
		fmt.Printf("Shard %d does not exist in balance map\n", walEntry.Shard)
		return &emptypb.Empty{}, nil
	}
	fmt.Printf("Reverting shard %d to balance %d\n", walEntry.Shard, walEntry.PreviousBalance)
	s.balance[int(walEntry.Shard)] = int(walEntry.PreviousBalance)
	fmt.Printf("Shard %d balance reverted to %d\n", walEntry.Shard, walEntry.PreviousBalance)

	s.LockOperation(false, int(req.Transaction.Sender), int(req.Transaction.Receiver))

	return &emptypb.Empty{}, nil
}
