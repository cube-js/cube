#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

operatorToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoib3BlcmF0b3IiLCJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.8LH7yCpWZ8wnaetJLJVVR6OYQzIGf8B4jdaOpbO9WsM
managerToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoibWFuYWdlciIsImlhdCI6MTAwMDAwMDAwMCwiZXhwIjo1MDAwMDAwMDAwfQ.3n17t_lTumC7Bc4uT7jrPjZMiGQ0rpfyy6fKil9WcC8

query=$(cat query/queries/query.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${managerToken}" -G -s --data-urlencode "query=${query}" -o managerResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${operatorToken}" -G -s --data-urlencode "query=${query}" -o operatorResponse.json

echo "Manager's data:"
jq ".data" managerResponse.json

echo "Operator's data:"
jq ".data" operatorResponse.json
