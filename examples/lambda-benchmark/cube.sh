#!/bin/sh

TAG=$1
CMD=$2

export CUBESTORE_DIR=${TAG}
export CUBEJS_TEST_PORT=4001
export CUBESTORE_TEST_PORT=3001
export CUBEJS_DEV_MODE=true
# export CUBEJS_TEST_USE_LAMBDA=

rm -Rf cube/.cubestore/${TAG}
docker-compose -p cubejs-${TAG} -f cube/docker-compose.yml ${CMD}
