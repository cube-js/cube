#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_MSSQL_VERSION=2017-latest

echo "::group::MSSQL ${TEST_MSSQL_VERSION}";
docker pull mcr.microsoft.com/mssql/server:${TEST_MSSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:mssql
echo "::endgroup::"

export TEST_MSSQL_VERSION=2019-latest

echo "::group::MSSQL ${TEST_MSSQL_VERSION}";
docker pull mcr.microsoft.com/mssql/server:${TEST_MSSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:mssql
echo "::endgroup::"
