package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	"google.golang.org/grpc"
)

func setUpClientServerRPC(id int) (pb.ClientServerConnectionClient, context.Context, *grpc.ClientConn) {
	fmt.Println("Port ", 5000+id)
	conn, err := grpc.Dial("localhost:"+strconv.Itoa(5000+id), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	c := pb.NewClientServerConnectionClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)
	return c, ctx, conn
}

// func SetupRpc(id int, server *Server) {
// 	lis, err := net.Listen("tcp", ":5005"+strconv.Itoa(id))
// 	if err != nil {
// 		log.Fatalf("Failed to listen: %v", err)
// 	}
// 	s := grpc.NewServer()
// 	pb.RegisterClientServerConnectionServer(s, server)
// 	log.Printf("Server listening at %v", lis.Addr())
// 	if err := s.Serve(lis); err != nil {
// 		log.Fatalf("Failed to serve: %v", err)
// 	}
// }
