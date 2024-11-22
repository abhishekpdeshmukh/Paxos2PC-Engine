package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	"google.golang.org/grpc"
)

// func setUpClientServerRPC(id int) (pb.ClientServerConnectionClient, context.Context, *grpc.ClientConn) {
// 	fmt.Println("Port ", 5000+id)
// 	conn, err := grpc.Dial("localhost:"+strconv.Itoa(5000+id), grpc.WithInsecure(), grpc.WithBlock())
// 	if err != nil {
// 		log.Fatalf("Could not connect: %v", err)
// 	}
// 	c := pb.NewClientServerConnectionClient(conn)
// 	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)
// 	return c, ctx, conn
// }

//	func SetupRpc(id int, server *Server) {
//		lis, err := net.Listen("tcp", ":5005"+strconv.Itoa(id))
//		if err != nil {
//			log.Fatalf("Failed to listen: %v", err)
//		}
//		s := grpc.NewServer()
//		pb.RegisterClientServerConnectionServer(s, server)
//		log.Printf("Server listening at %v", lis.Addr())
//		if err := s.Serve(lis); err != nil {
//			log.Fatalf("Failed to serve: %v", err)
//		}
//	}
func setUpClientServerRPC(serverID int) (pb.ClientServerConnectionClient, context.Context, *grpc.ClientConn) {
	// Find server details using serverID
	var server Server
	for _, servers := range clusterToServers {
		for _, s := range servers {
			if s.ServerID == serverID {
				server = s
				break
			}
		}
	}
	if server.ServerID == 0 {
		log.Fatalf("Server with ID %d not found", serverID)
	}

	address := fmt.Sprintf("%s:%d", server.IP, server.Port)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to server %d: %v", serverID, err)
	}
	client := pb.NewClientServerConnectionClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), 600*time.Second)
	return client, ctx, conn
}
