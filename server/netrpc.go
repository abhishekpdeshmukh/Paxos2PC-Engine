package main

import (
	"log"
	"net"
	"strconv"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
	"google.golang.org/grpc"
)

// func setUpClientServerSender(id int) (pb.ClientServerConnectionClient, context.Context, *grpc.ClientConn) {
// 	// //fmt.Println("Setting Up RPC Reciever")
// 	conn, err := grpc.Dial("localhost:"+strconv.Itoa((5005+id+5)), grpc.WithInsecure(), grpc.WithBlock())
// 	if err != nil {
// 		log.Fatalf("Could not connect: %v", err)
// 	}
// 	// defer conn.Close()
// 	c := pb.ClientServerConnectionClient(conn)

// 	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
// 	// cancel()
// 	return c, ctx, conn
// }

func setUpClientServerReceiver(server *Server) {
	lis, err := net.Listen("tcp", ":5000"+strconv.Itoa(server.ServerID))
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
