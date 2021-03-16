#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_PRESTO_VERSION=341-SNAPSHOT
export TEST_PGSQL_VERSION=12.4

echo "::group::PrestoDB ${TEST_PRESTO_VERSION} with PostgreSQL ${TEST_PGSQL_VERSION}"
docker pull lewuathe/presto-coordinator:${TEST_PRESTO_VERSION}
docker pull lewuathe/presto-worker:${TEST_PRESTO_VERSION}
docker pull postgres:${TEST_PGSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:presto
echo "::endgroup::"
