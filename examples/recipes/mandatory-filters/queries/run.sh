#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

query=$(cat query/queries/query.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${query}" -o managerResponse.json
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${query}" -o operatorResponse.json

echo "Manager's data:"
jq ".data" managerResponse.json

echo "Operator's data:"
jq ".data" operatorResponse.json
