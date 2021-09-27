#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw

query=$(cat query/queries/orders.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${query}" -o firstResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${query}" -o secondResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${query}" -o thirdResponse.json

echo "First:"
jq ".data" firstResponse.json

echo "First pre-aggregations:"
jq ".usedPreAggregations" firstResponse.json

echo "---------"

echo "Second:"
jq ".data" secondResponse.json

echo "Second pre-aggregations:"
jq ".usedPreAggregations" secondResponse.json

echo "---------"

echo "Third:"
jq ".data" thirdResponse.json

echo "Third pre-aggregations:"
jq ".usedPreAggregations" thirdResponse.json
