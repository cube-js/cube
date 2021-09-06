#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

firstEmailToken=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMCwiZW1haWwiOiJwdXJ1cy5hY2N1bXNhbkBQcm9pbi5vcmcifQ.bh8rmIvjLuJG6_jodZZxY1s9f79t0oHRftRAdmEY2BE
secondEmailToken=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMCwiZW1haWwiOiJncmF2aWRhLnNpdC5hbWV0QHJpc3VzLm5ldCJ9.GXX-wo5uqddal_1j3RQn4OnH6aOUAhwen1ocPynuq-s

query=$(cat query/queries/query.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl"  > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${firstEmailToken}" -G -s --data-urlencode "query=${query}" -o firstEmailResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${secondEmailToken}" -G -s --data-urlencode "query=${query}"  -o secondEmailResponse.json

echo "purus.accumsan@Proin.org's data:"
jq ".data" firstEmailResponse.json

echo "gravida.sit.amet@risus.net's data:"
jq ".data" secondEmailResponse.json
