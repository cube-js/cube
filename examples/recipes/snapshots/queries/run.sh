#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw

statusesQuery=$(cat query/queries/statuses.json)
snapshotQuery1=$(cat query/queries/snapshot-1.json)
snapshotQuery2=$(cat query/queries/snapshot-2.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the queries
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${statusesQuery}" -o statusesResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${snapshotQuery1}" -o snapshotResponse1.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${snapshotQuery2}" -o snapshotResponse2.json

echo "Statuses (excerpt):"
jq ".data" statusesResponse.json

echo "Shipped as of April 1, 2019:"
jq ".data" snapshotResponse1.json

echo "Shipped as of May 1, 2019:"
jq ".data" snapshotResponse2.json