#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

percentageQuery=$(cat query/queries/percentage.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the queries
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${percentageQuery}" -o percentageResponse.json

echo "Percent distribution by statuses"
jq ".data" percentageResponse.json
