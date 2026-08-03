#!/bin/bash
set -eo pipefail

# Cube officially maintained upstream LTS releases (as of 2026-06): 25.8, 26.3. + 1 EOL release
# 24.8 can be removed, but let's test that it still works

# Debug log for test containers
export DEBUG=testcontainers

export TEST_CLICKHOUSE_VERSION=26.3

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull clickhouse/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"

export TEST_CLICKHOUSE_VERSION=25.8

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull clickhouse/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"

export TEST_CLICKHOUSE_VERSION=25.3

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull clickhouse/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"

export TEST_CLICKHOUSE_VERSION=24.8

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull clickhouse/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"
