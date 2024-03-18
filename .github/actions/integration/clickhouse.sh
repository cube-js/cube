#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_CLICKHOUSE_VERSION=23.11

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull clickhouse/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"

export TEST_CLICKHOUSE_VERSION=22.8

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull clickhouse/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"

export TEST_CLICKHOUSE_VERSION=21.8

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull clickhouse/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"
