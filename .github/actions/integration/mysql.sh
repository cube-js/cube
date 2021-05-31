#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_MYSQL_VERSION=5.6

echo "::group::MySQL ${TEST_MYSQL_VERSION}";
docker pull mysql:${TEST_MYSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:mysql
echo "::endgroup::"

export TEST_MYSQL_VERSION=5.7

echo "::group::MySQL ${TEST_MYSQL_VERSION}";
docker pull mysql:${TEST_MYSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:mysql
echo "::endgroup::"

export TEST_MYSQL_VERSION=8.0.24

echo "::group::MySQL ${TEST_MYSQL_VERSION}";
docker pull mysql:${TEST_MYSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:mysql
echo "::endgroup::"
