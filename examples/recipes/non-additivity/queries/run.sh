#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw

matchingNonAdditiveQuery=$(cat query/queries/matching-non-additive.json)
nonMatchingNonAdditiveQuery=$(cat query/queries/non-matching-non-additive.json)
matchingRefactoredQuery=$(cat query/queries/matching-refactored.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the queries
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${matchingNonAdditiveQuery}" -o matchingNonAdditiveResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${nonMatchingNonAdditiveQuery}" -o nonMatchingNonAdditiveResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${token}" -G -s --data-urlencode "query=${matchingRefactoredQuery}" -o matchingRefactoredResponse.json

echo "Matching (thus, accelerated) non-additive query:"
echo "$matchingNonAdditiveQuery" | jq '.'
echo "Matched pre-aggregation:"
jq ".usedPreAggregations" matchingNonAdditiveResponse.json

echo ""
echo "Non-matching (thus, non-accelerated) non-additive query:"
echo "$nonMatchingNonAdditiveQuery" | jq '.'
echo "Matched pre-aggregation:"
jq ".usedPreAggregations" nonMatchingNonAdditiveResponse.json

echo ""
echo "Matching (thus, accelerated) query against the refactored cube:"
echo "$matchingRefactoredQuery" | jq '.'
echo "Matched pre-aggregation:"
jq ".usedPreAggregations" matchingRefactoredResponse.json