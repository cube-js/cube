#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_PGSQL_VERSION=12

echo "::group::PostgreSQL ${TEST_PGSQL_VERSION}"
docker pull postgres:${TEST_PGSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:postgres
echo "::endgroup::"
