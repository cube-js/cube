#!/bin/bash

# JWT decode function https://gist.github.com/a-magdy/a771f1426043ab4b66b19cc9a652908b
_decode_base64_url() {
  local len=$((${#1} % 4))
  local result="$1"
  if [ $len -eq 2 ]; then result="$1"'=='
  elif [ $len -eq 3 ]; then result="$1"'=' 
  fi
  echo "$result" | tr '_-' '/+' | base64 -d
}
decode_jwt() { _decode_base64_url $(echo -n $1 | cut -d "." -f ${2:-2}) | jq .; }

host=cube
port=4000
loadUrl=cubejs-api/v1/load
readyzUrl=readyz

avocadoToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidGVuYW50IjoiQXZvY2FkbyIsImlhdCI6MTAwMDAwMDAwMCwiZXhwIjo1MDAwMDAwMDAwfQ.0I17p0gd6cKEqYHzwwaI2tbrQASSf7C7cIO8Jnh_WqE
mangoToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidGVuYW50IjoiTWFuZ28iLCJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.RBhdka-5MVR-y1vzbMFZrmfdX0PL_l1vb5G7OjM8hjU

query=$(cat query/queries/query.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${avocadoToken}" -G -s --data-urlencode "query=${query}" -o avocadoResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${mangoToken}" -G -s --data-urlencode "query=${query}" -o mangoResponse.json


echo "Avocado JWT payload:"
decode_jwt $avocadoToken

echo "Avocado products:"
jq ".data" avocadoResponse.json

echo "---------"

echo "Mango JWT payload:"
decode_jwt $mangoToken

echo "Mango products:"
jq ".data" mangoResponse.json
