package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	"google.golang.org/grpc"
)

func setUpServerServerSender(id int) (pb.PaxosServiceClient, context.Context, *grpc.ClientConn) {
	// //fmt.Println("Setting Up RPC Reciever")
	// conn, err := grpc.Dial("localhost:"+strconv.Itoa((500+id+30)), grpc.WithInsecure(), grpc.WithBlock())
	conn, err := grpc.Dial("localhost:"+strconv.Itoa((5000+id+30)), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	// defer conn.Close()
	c := pb.NewPaxosServiceClient(conn)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*100)
	return c, ctx, conn
}

func setUpClientServerReceiver(server *Server) {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(5000+server.ServerID))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterClientServerConnectionServer(s, server)
	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
func setUpServerServerReceiver(server *Server) {
	fmt.Println("Setting Up SERVER to SERVER communication")
	fmt.Println(5000+server.ServerID+10)
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(5000+server.ServerID+30))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPaxosServiceServer(s, server)
	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
