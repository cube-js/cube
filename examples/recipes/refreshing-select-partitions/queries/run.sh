#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw

ordersQuery=$(cat query/queries/orders.json)
updatedOrdersQuery=$(cat query/queries/updated-orders.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${ordersQuery}" -o ordersResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${updatedOrdersQuery}" -o updatedOrdersResponse.json

echo "Orders:"
jq ".data" ordersResponse.json

echo "Orders pre-aggregations:"
jq ".usedPreAggregations" ordersResponse.json

echo "---------"

echo "Updated orders:"
jq ".data" updatedOrdersResponse.json

echo "Updated Orders pre-aggregations:"
jq ".usedPreAggregations" updatedOrdersResponse.json


# Wait for the order update
sleep 10

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${updatedOrdersQuery}" -o updatedOrdersResponse.json

echo "Updated orders new:"
jq ".data" updatedOrdersResponse.json

echo "Updated Orders new pre-aggregations:"
jq ".usedPreAggregations" updatedOrdersResponse.json
