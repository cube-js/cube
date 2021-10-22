#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw

preAggQuery=$(cat query/queries/pre-agg-query.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the queries
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${preAggQuery}" -o preAggResponse.json

echo "Product suppliers:"
jq ".data" preAggResponse.json

echo "Names of the used pre-aggregations:"
jq ".usedPreAggregations" preAggResponse.json
