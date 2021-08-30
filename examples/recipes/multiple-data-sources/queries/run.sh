#!/bin/bash

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

cubeDevtoken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidGVuYW50IjoiY3ViZURldiIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNzI0OTk1NTgxfQ.i9n4vjnuL3-Ly9aWTbj9pdZQWZpxrle6KIStmD3huqI
cubeIncToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidGVuYW50IjoiY3ViZUluYyIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNzI0OTk1NTgxfQ.JWoqbgonhuGhMj0aTIxa4DuZTy1EksN7CaxyF7zi5qE

query=$(cat query/queries/users.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${cubeIncToken}" -G -s --data-urlencode "query=${query}" -o cubeIncResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${cubeDevtoken}" -G -s --data-urlencode "query=${query}" -o cubeDevResponse.json

echo "Cube Inc last users:"
jq ".data" cubeIncResponse.json

echo "Cube Dev last users:"
jq ".data" cubeDevResponse.json
