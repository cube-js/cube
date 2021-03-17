#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_CLICKHOUSE_VERSION=21.1.2

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull yandex/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"

export TEST_CLICKHOUSE_VERSION=20.6

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull yandex/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"

export TEST_CLICKHOUSE_VERSION=19

echo "::group::Clickhouse ${TEST_CLICKHOUSE_VERSION}";
docker pull yandex/clickhouse-server:${TEST_CLICKHOUSE_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:clickhouse
echo "::endgroup::"
