#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

usersQuery=$(cat query/queries/users.json)
completedOrdersQuery=$(cat query/queries/completed-orders.json)
shippedOrdersQuery=$(cat query/queries/shipped-orders.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${completedOrdersQuery}" -o completedResponse.json
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${shippedOrdersQuery}" -o shippedResponse.json

echo "Completed orders created after 30 Dec 2019:"
jq ".data" completedResponse.json

echo "Shipped orders created after 30 Dec 2019:"
jq ".data" shippedResponse.json
