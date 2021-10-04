#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhIjoiYTEiLCJpYXQiOjE2MzAzNTQ5MDZ9.4A2x80H8C_6QkslIFV8Nu5-6sRcfCAcyi4zc4DXqu_Q

query=$(cat query/queries/query.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl"  > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${query}" -o response.json

jq ".data" response.json
