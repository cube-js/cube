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

testingToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZW52IjoidGVzdGluZyIsImlhdCI6MTAwMDAwMDAwMCwiZXhwIjo1MDAwMDAwMDAwfQ.RNr0eInvGE3X3pivhcmP60XStKuNWl7lV0xZO30yUmQ
stagingToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZW52Ijoic3RhZ2luZyIsImlhdCI6MTAwMDAwMDAwMCwiZXhwIjo1MDAwMDAwMDAwfQ.o_kQ5kqF98ufgAouUNF4PSXMjdL8mKQmLm6FQZeGdZE
productionToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZW52IjoicHJvZHVjdGlvbiIsImlhdCI6MTAwMDAwMDAwMCwiZXhwIjo1MDAwMDAwMDAwfQ.vdO5Z4ttp2x1_d6vpR3wcfaUj9AxXRMQ_iqKSyTlP94

query=$(cat query/queries/query.json)

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

# Send the query
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${testingToken}" -G -s --data-urlencode "query=${query}" -o testingResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${stagingToken}" -G -s --data-urlencode "query=${query}" -o stagingResponse.json
curl "$host":"$port"/"$loadUrl" -H "Authorization: ${productionToken}" -G -s --data-urlencode "query=${query}" -o productionResponse.json


echo "Testing env JWT payload:"
decode_jwt $testingToken

echo "Response from query to testing:"
jq ".data" testingResponse.json

echo "Names of the used pre-aggregations:"
jq ".usedPreAggregations" testingResponse.json

echo "---------"

echo "Staging env JWT payload:"
decode_jwt $stagingToken

echo "Response from query to staging:"
jq ".data" stagingResponse.json

echo "Names of the used pre-aggregations:"
jq ".usedPreAggregations" stagingResponse.json

echo "---------"

echo "Production env JWT payload:"
decode_jwt $productionToken

echo "Response from query to production:"
jq ".data" productionResponse.json

echo "Names of the used pre-aggregations:"
jq ".usedPreAggregations" productionResponse.json
