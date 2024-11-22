#!/bin/bash

CONFIG_FILE="../config.json"

CLUSTERS=$(jq -c '.clusters[]' $CONFIG_FILE)

for cluster in $CLUSTERS; do
  CLUSTER_ID=$(echo $cluster | jq '.id')
  
  SERVERS=$(echo $cluster | jq -c '.servers[]')
  
  for server in $SERVERS; do
    SERVER_ID=$(echo $server | jq '.serverId')
    PORT=$((5000 + SERVER_ID))  # Calculate the port number

    # Find the PID of the process running on the given port
    PID=$(lsof -t -i :"$PORT")

    if [ -n "$PID" ]; then
      # Kill the process running on the port
      echo "Killing server $SERVER_ID running on port $PORT with PID $PID..."
      kill -9 "$PID" || echo "Failed to kill server $SERVER_ID with PID $PID"

      # Find the parent terminal process and close it
      TERMINAL_PID=$(ps -o ppid= -p "$PID" | tr -d ' ')
      if [ -n "$TERMINAL_PID" ]; then
        echo "Closing terminal with PID $TERMINAL_PID..."
        kill -9 "$TERMINAL_PID" || echo "Failed to close terminal with PID $TERMINAL_PID"
      fi
    else
      echo "No server found running on port $PORT for server $SERVER_ID"
    fi
  done
done
