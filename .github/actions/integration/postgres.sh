#!/bin/bash
set -eo pipefail

export TEST_PGSQL_VERSION=9.6

echo "::group::PostgreSQL ${TEST_PGSQL_VERSION}"
docker pull postgres:${TEST_PGSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:postgres
echo "::endgroup::"

export TEST_PGSQL_VERSION=10

echo "::group::PostgreSQL ${TEST_PGSQL_VERSION}"
docker pull postgres:${TEST_PGSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:postgres
echo "::endgroup::"

export TEST_PGSQL_VERSION=11

echo "::group::PostgreSQL ${TEST_PGSQL_VERSION}"
docker pull postgres:${TEST_PGSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:postgres
echo "::endgroup::"

export TEST_PGSQL_VERSION=12

echo "::group::PostgreSQL ${TEST_PGSQL_VERSION}"
docker pull postgres:${TEST_PGSQL_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:postgres
echo "::endgroup::"
