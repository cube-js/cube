#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw

statusesQuery=$(cat query/queries/statuses.json)
joinsQuery=$(cat query/queries/joins.json)
dryQuery=$(cat query/queries/dry.json)
dynamicQuery=$(cat query/queries/dynamic.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${statusesQuery}" -o statusesResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${joinsQuery}" -o joinsResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${dryQuery}" -o dryResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${dynamicQuery}" -o dynamicResponse.json

echo "Statuses:"
jq ".data" statusesResponse.json

echo "EAV with joins (excerpt):"
jq ".data" joinsResponse.json

echo "EAV with a DRY definition (excerpt):"
jq ".data" dryResponse.json

echo "EAV with a dynamic definition (excerpt):"
jq ".data" dynamicResponse.json
