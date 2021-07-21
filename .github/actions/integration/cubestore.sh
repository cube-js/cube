#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_CUBESTORE_VERSION=latest

echo "::group::CubeStore ${TEST_CUBESTORE_VERSION}";
docker pull cubejs/cubestore:${TEST_CUBESTORE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:cubestore
echo "::endgroup::"
