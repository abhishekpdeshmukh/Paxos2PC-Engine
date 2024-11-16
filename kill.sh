#!/bin/bash

CONFIG_FILE="../config.json"

CLUSTERS=$(jq -c '.clusters[]' $CONFIG_FILE)

for cluster in $CLUSTERS; do
  CLUSTER_ID=$(echo $cluster | jq '.id')
  
  SERVERS=$(echo $cluster | jq -c '.servers[]')
  
  for server in $SERVERS; do
    SERVER_ID=$(echo $server | jq '.serverId')
    PORT=$((5000 + SERVER_ID))  # Calculate the port number

    # Send an RPC request to terminate the server
    echo "Killing server $SERVER_ID at localhost:$PORT..."
    
    # Placeholder for the actual RPC call to kill the server
    # Replace this line with the actual command to send the RPC
    # For example, you could use a curl command or a custom Go program
    curl -X POST http://localhost:$PORT/kill || echo "Failed to kill server $SERVER_ID at localhost:$PORT"
  done
done
