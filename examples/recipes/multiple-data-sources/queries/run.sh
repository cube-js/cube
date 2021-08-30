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

avocadotoken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidGVuYW50IjoiQXZvY2FkbyBJbmMiLCJpYXQiOjE1MTYyMzkwMjIsImV4cCI6MTcyNDk5NTU4MX0.MZUdOATD7c7A6tEQDU_Iq0VI0TJUzO-PWmsFgkZ76uM
mangoToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidGVuYW50IjoiTWFuZ28gSW5jIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjE3MjQ5OTU1ODF9.lcXMKZoWa94Bxexq0J96AqKvJAAxYyjwPJFl7tGYUl8

query=$(cat query/queries/users.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${avocadotoken}" -G -s --data-urlencode "query=${query}" -o avocadoResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${mangoToken}" -G -s --data-urlencode "query=${query}" -o mangoResponse.json


echo "Avocado Inc JWT payload:"
decode_jwt $avocadotoken

echo "Avocado Inc last users:"
jq ".data" avocadoResponse.json

echo "---------"

echo "Mango Inc JWT payload:"
decode_jwt $mangoToken

echo "Mango Inc last users:"
jq ".data" mangoResponse.json
