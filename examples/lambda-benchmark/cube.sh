#!/bin/sh

TAG=$1

# export CUBEJS_DEV_MODE=true
export CUBESTORE_DIR=${TAG}
case ${TAG} in
  basic)
    export CUBEJS_TEST_PORT=4001
    export CUBESTORE_TEST_PORT=3001
    ;;
  lambda)
    export CUBEJS_TEST_PORT=4002
    export CUBESTORE_TEST_PORT=3002
    export CUBEJS_TEST_USE_LAMBDA=true
    ;;
esac

rm -Rf cube/.cubestore/${TAG}
mkdir -p cube/.cubestore/${TAG}
docker-compose -p cubejs-${TAG} -f cube/docker-compose.yml down
docker-compose -p cubejs-${TAG} -f cube/docker-compose.yml up
