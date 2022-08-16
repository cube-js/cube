#!/bin/sh

export CUBEJS_TEST_PORT=4001
export CUBESTORE_TEST_PORT=3001
export CUBEJS_TEST_USE_LAMBDA=

docker-compose -p cubejs-basic -f cube/docker-compose.yml up