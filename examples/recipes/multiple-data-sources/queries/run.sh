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

avocadoToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidGVuYW50IjoiQXZvY2FkbyBJbmMiLCJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.AJ6DwEbbuMzGBQjtQKtC_lFvOKtirTz7q4m4RT5cVAs
mangoToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidGVuYW50IjoiTWFuZ28gSW5jIiwiaWF0IjoxMDAwMDAwMDAwLCJleHAiOjUwMDAwMDAwMDB9.iDG8MTmGGRvJU0lfuSlIZZVpJTiVyZuglYVsFnE26mQ
peachToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwidGVuYW50IjoiUGVhY2ggSW5jIiwiaWF0IjoxMDAwMDAwMDAwLCJleHAiOjUwMDAwMDAwMDB9.zs9xM9Z_QxrTXmHq9f_pCPaJ-DWW7GbKByQ0jMTrHd0

query=$(cat query/queries/users.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${avocadoToken}" -G -s --data-urlencode "query=${query}" -o avocadoResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${mangoToken}" -G -s --data-urlencode "query=${query}" -o mangoResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${peachToken}" -G -s --data-urlencode "query=${query}" -o peachResponse.json


echo "Avocado Inc JWT payload:"
decode_jwt $avocadoToken

echo "Avocado Inc last users:"
jq ".data" avocadoResponse.json

echo "---------"

echo "Mango Inc JWT payload:"
decode_jwt $mangoToken

echo "Mango Inc last users:"
jq ".data" mangoResponse.json

echo "---------"

echo "Peach Inc JWT payload:"
decode_jwt $peachToken

echo "Peach Inc error:"
echo  "$(<peachResponse.json)"
