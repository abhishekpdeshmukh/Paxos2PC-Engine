module github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE/proto

replace github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE => ./

go 1.23.2

require (
	github.com/abhishekpdeshmukh/PAXOS2PC-ENGINE v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.4
	google.golang.org/grpc v1.68.0
	google.golang.org/protobuf v1.35.2
)

require (
	github.com/mattn/go-sqlite3 v1.14.24 // indirect
	golang.org/x/net v0.29.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	golang.org/x/text v0.18.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
)
