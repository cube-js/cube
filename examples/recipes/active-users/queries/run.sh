#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

query=$(cat query/queries/query.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl"  > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${query}" -o usersResponse.json

echo "Daily, weekly, and monthly active users"
jq ".data" usersResponse.json
