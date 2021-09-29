#!/bin/bash

host=cube
port=4000
readyzUrl=readyz

# Wait for the Cube API to become ready
until curl -s "$host":"$port"/"$readyzUrl" > /dev/null; do
  sleep 1
done

sleep 15

node node/fetch.js
