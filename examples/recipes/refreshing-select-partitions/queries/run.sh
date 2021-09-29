#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw

ordersQuery=$(cat query/queries/orders.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${ordersQuery}" -o ordersResponse.json

echo "Orders before update:"
jq ".data" ordersResponse.json

echo "Pre-aggregations for orders before update:"
jq ".usedPreAggregations" ordersResponse.json

# Wait for the order update
sleep 10

# Send the query for updated orders
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${ordersQuery}" -o updatedOrdersResponse.json

echo "Orders after update:"
jq ".data" updatedOrdersResponse.json

echo "Pre-aggregations for orders after update:"
jq ".usedPreAggregations" updatedOrdersResponse.json
