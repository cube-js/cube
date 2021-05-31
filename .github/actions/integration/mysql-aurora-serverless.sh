#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_MYSQL_VERSION=5.6.50
export TEST_LOCAL_DATA_API_VERSION=0.6.4

echo "::group::MySQL ${TEST_MYSQL_VERSION} Data Api ${TEST_LOCAL_DATA_API_VERSION}";
docker pull mysql:${TEST_MYSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:mysql-aurora-serverless
echo "::endgroup::"
