# #!/bin/bash

# CONFIG_FILE="../config.json"

# CLUSTERS=$(jq -c '.clusters[]' $CONFIG_FILE)

# for cluster in $CLUSTERS; do
#   CLUSTER_ID=$(echo $cluster | jq '.id')
  
#   # Extract shard range and explicit IDs
#   SHARD_RANGE=$(echo $cluster | jq -r '.shard.range // empty')
#   EXPLICIT_IDS=$(echo $cluster | jq -r '.shard.explicitIds | @csv // empty')
  
#   # Create the SHARD_ITEMS string
#   if [ -n "$SHARD_RANGE" ]; then
#     # If SHARD_RANGE is not empty, parse it
#     IFS="-" read -r START END <<< "$SHARD_RANGE"
#     SHARD_ITEMS=$(seq -s " " $START $END)  # Generate a comma-separated list of numbers
#   elif [ -n "$EXPLICIT_IDS" ]; then
#     # If EXPLICIT_IDS is not empty, use it directly
#     SHARD_ITEMS=$(echo $EXPLICIT_IDS | sed 's/,/ /g')  # Replace commas with spaces
#   else
#     SHARD_ITEMS=""
#   fi
  
#   SERVERS=$(echo $cluster | jq -c '.servers[]')
  
#   for server in $SERVERS; do
#     SERVER_ID=$(echo $server | jq '.serverId')
#     IP=$(echo $server | jq -r '.ip')
#     PORT=$(echo $server | jq '.port')

#     # Spawn the server process with the shard items
#     gnome-terminal -- bash -c "go run . --clusterId $CLUSTER_ID --serverId $SERVER_ID --ip $IP --port $PORT --shardItems \'$SHARD_ITEMS\'; exit"
#   done
# done


#!/bin/bash

CONFIG_FILE="../config.json"

CLUSTERS=$(jq -c '.clusters[]' "$CONFIG_FILE")

for cluster in $CLUSTERS; do
  CLUSTER_ID=$(echo "$cluster" | jq '.id')
  
  # Extract shard range and explicit IDs
  SHARD_RANGE=$(echo "$cluster" | jq -r '.shard.range // empty')
  EXPLICIT_IDS=$(echo "$cluster" | jq -r '.shard.explicitIds // empty')
  
  # Create the SHARD_ITEMS string
  if [ -n "$SHARD_RANGE" ]; then
    # If SHARD_RANGE is not empty, parse it
    IFS="-" read -r START END <<< "$SHARD_RANGE"
    SHARD_ITEMS=$(seq -s " " "$START" "$END")  # Generate a space-separated list of numbers
  elif [ "$EXPLICIT_IDS" != "null" ] && [ -n "$EXPLICIT_IDS" ]; then
    # If EXPLICIT_IDS is not null or empty, use it directly
    SHARD_ITEMS=$(echo "$EXPLICIT_IDS" | jq -r '. | join(" ")')  # Join array items with spaces
  else
    SHARD_ITEMS=""
  fi
  
  SERVERS=$(echo "$cluster" | jq -c '.servers[]')
  SERVER_IDS=$(echo "$cluster" | jq -r '.servers[].serverId' | paste -sd "," -)
  for server in $SERVERS; do
    SERVER_ID=$(echo "$server" | jq '.serverId')
    IP=$(echo "$server" | jq -r '.ip')
    PORT=$(echo "$server" | jq '.port')

    # Spawn the server process with the shard items
    gnome-terminal -- bash -c "go run . --clusterId $CLUSTER_ID --serverId $SERVER_ID --ip $IP --port $PORT --shardItems \"$SHARD_ITEMS\" --serverIds \"$SERVER_IDS\"; exit"
  done
done
