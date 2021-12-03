#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
metaUrl=cubejs-api/v1/meta
readyzUrl=readyz

token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw

query=$(cat query/queries/load.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${query}" -o loadResponse.json
curl "$host":"$port"/"$metaUrl" -H "Authorization: ${token}" -G -s -o metaResponse.json

echo "Users' city unique values:"
jq ".data" loadResponse.json

echo "Information about cubes, dimensions included:"
jq "." metaResponse.json
