package main

type Shard struct {
	Range       string `json:"range"`       // Range as a string, e.g., "1-1000"
	ExplicitIds []int  `json:"explicitIds"` // List of explicit IDs
}

type ServerConfig struct {
	ServerID int    `json:"serverId"` // Unique server ID
	IP       string `json:"ip"`       // IP address of the server
	Port     int    `json:"port"`     // Port number of the server
}

type Cluster struct {
	ID      int            `json:"id"`      // Cluster ID
	Shard   Shard          `json:"shard"`   // Shard information
	Servers []ServerConfig `json:"servers"` // List of servers in the cluster
}

type Config struct {
	Clusters []Cluster `json:"clusters"` // List of clusters
}
type Reply struct {
	Yes bool
}
