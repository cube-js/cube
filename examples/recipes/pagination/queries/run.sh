#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

countQuery=$(cat query/queries/count.json)
firstQuery=$(cat query/queries/first.json)
secondQuery=$(cat query/queries/second.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${countQuery}" -o countResponse.json
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${firstQuery}" -o firstResponse.json
curl "$host":"$port"/"$loadUrl" -G -s --data-urlencode "query=${secondQuery}" -o secondResponse.json

echo "Orders count:"
jq ".data" countResponse.json

echo "The first five orders:"
jq ".data" firstResponse.json

echo "The second five orders:"
jq ".data" secondResponse.json
