#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

operatorToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoib3BlcmF0b3IiLCJpYXQiOjE2Mjg3NDUwNDUsImV4cCI6MTgwMTU0NTA0NX0.VErb2t7Bc43ryRwaOiEgXuU5KiolCT-69eI_i2pRq4o
managerToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoibWFuYWdlciIsImlhdCI6MTYyODc0NTAxMSwiZXhwIjoxODAxNTQ1MDExfQ.1cOAjRHhrFKD7Tg3g57ppVm5nX4eI0zSk8JMbinfzTk

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
