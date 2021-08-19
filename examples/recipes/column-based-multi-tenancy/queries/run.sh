#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

firstEmailToken=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiIiLCJpYXQiOjE2MjkyNjY0NzAsImV4cCI6MTY5MjMzODQ3MCwiYXVkIjoiIiwic3ViIjoiIiwiZW1haWwiOiJwdXJ1cy5hY2N1bXNhbkBQcm9pbi5vcmcifQ.vA_pzTOBYS10D2mhno0COJux7hhchfNmx-eh52SwSko
secondEmailToken=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiIiLCJpYXQiOjE2MjkyNjY0NzAsImV4cCI6MTY5MjMzODQ3MCwiYXVkIjoiIiwic3ViIjoiIiwiZW1haWwiOiJncmF2aWRhLnNpdC5hbWV0QHJpc3VzLm5ldCJ9.ZOkiky821CZwoNi3VTcTsiiULl5tBkjmgX-1uW0UEjA

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
