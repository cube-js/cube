#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

# Recursive CTE based generated time series require MySQL 8.0+.
# Keep it disabled for 5.6/5.7 (falls back to the portable VALUES series) and
# enable it only for the 8.0.x run below.
export CUBEJS_DB_MYSQL_USE_GENERATED_TIME_SERIES=false

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
export CUBEJS_DB_MYSQL_USE_GENERATED_TIME_SERIES=true

echo "::group::MySQL ${TEST_MYSQL_VERSION}";
docker pull mysql:${TEST_MYSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:mysql
echo "::endgroup::"
