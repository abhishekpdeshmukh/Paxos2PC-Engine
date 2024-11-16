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

type Server struct {
	pb.UnimplementedClientServerConnectionServer
	ServerID   int
	ClusterID  int
	Port       int
	IP         string
	ShardItems string
	lockMap    map[int]bool
	lock       sync.Mutex
	db         *sql.DB
	isActive   bool
}

func main() {
	// Check if there are enough arguments
	if len(os.Args) < 9 {
		fmt.Println("Usage: go run . --clusterId <id> --serverId <id> --ip <ip> --port <port> --shardItems <items>")
		return
	}

	// Parse command-line arguments
	var clusterId, serverId, ip, port, shardItems string
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
	tempMap := make(map[int]bool)
	for _, item := range shardItemsList {
		shardID, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil {
			fmt.Println("Invalid shard item:", item)
			continue
		}
		tempMap[shardID] = false
	}
	// Fill the Server struct
	server := &Server{
		ServerID:   serverIDInt,
		ClusterID:  clusterIDInt,
		Port:       portInt,
		IP:         ip,
		ShardItems: shardItems,
		lockMap:    tempMap,
		isActive:   true,
	}

	// Print the filled struct
	// fmt.Printf("Server Struct: %+v\n", server)
	printServer(server)
	setUpClientServerReceiver(server)
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
