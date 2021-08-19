#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

usersQuery=$(cat query/queries/users.json)
ordersQuery=$(cat query/queries/orders.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${usersQuery}" -o usersResponse.json
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${ordersQuery}" -o ordersResponse.json

echo "Users created after 30 Dec 2019:"
jq ".data" usersResponse.json

echo "Orders created after 30 Dec 2019:"
jq ".data" ordersResponse.json
