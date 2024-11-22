package main

import (
	"fmt"

	pb "github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto"
)

func (s *Server) BroadCastPrepare(txn *pb.Transaction) {
	s.lock.Lock()
	s.lock.Unlock()
	s.ballotNum++
	// var wg sync.WaitGroup
	fmt.Println("Starting my Paxos with Ballot Number ", s.ballotNum)
	promiseCount := 0
	fmt.Println(s.servers)
	for _, i := range s.servers {
		if(i!= s.ServerID){
			fmt.Println(i)
			go func(n int) {
				c, ctx, conn := setUpServerServerSender(n)
				defer conn.Close()

				// Make Prepare request
				promise, err := c.Prepare(ctx, &pb.PrepareRequest{
					Ballot: &pb.Ballot{
						BallotNum: int32(s.ballotNum),
						NodeID:    int32(s.ServerID),
					},
					LogSize: int32(len(s.transactionLog)),
				})
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println(promise)
					promiseCount++
				}

			}(i)
		}

	}
	for promiseCount < (len(s.servers)/2)+1 {

	}
	fmt.Println("Got Enough Promises")
}
