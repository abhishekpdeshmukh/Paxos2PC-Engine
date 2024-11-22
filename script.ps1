# Path to the configuration file
$CONFIG_FILE = "../config.json"

# Read the JSON content from the file
$CLUSTERS = Get-Content $CONFIG_FILE | ConvertFrom-Json | Select-Object -ExpandProperty clusters

foreach ($cluster in $CLUSTERS) {
    $CLUSTER_ID = $cluster.id

    # Extract shard range and explicit IDs
    $SHARD_RANGE = $cluster.shard.range
    $EXPLICIT_IDS = $cluster.shard.explicitIds

    # Initialize SHARD_ITEMS
    $SHARD_ITEMS = ""

    # Create the SHARD_ITEMS string
    if ($SHARD_RANGE) {
        # If SHARD_RANGE is not empty, parse it
        $START, $END = $SHARD_RANGE -split '-'
        $SHARD_ITEMS = ( $START..$END ) -join ' '  # Use the PowerShell range operator
    }
    elseif ($EXPLICIT_IDS) {
        # If EXPLICIT_IDS is not empty, use it directly
        $SHARD_ITEMS = ($EXPLICIT_IDS -join ' ') -replace ',', ' '
    }

    $SERVERS = $cluster.servers
    $SERVER_IDS = ($cluster.servers | ForEach-Object { $_.serverId }) -join ','
    foreach ($server in $SERVERS) {
        $SERVER_ID = $server.serverId
        $IP = $server.ip
        $PORT = $server.port

        # Spawn the server process with the shard items
        Start-Process "powershell" -ArgumentList "-NoExit", "-Command", "go run . --clusterId $CLUSTER_ID --serverId $SERVER_ID --ip $IP --port $PORT --shardItems '$SHARD_ITEMS' --serverIds '$SERVER_IDS'"
    }
}
