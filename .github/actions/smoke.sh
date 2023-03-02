#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::Oracle"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:oracle
echo "::endgroup::"

echo "::group::Postgres"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:postgres
echo "::endgroup::"

echo "::group::QuestDB"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:questdb
echo "::endgroup::"

echo "::group::Crate"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:crate
echo "::endgroup::"

echo "::group::Lambda"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:lambda
echo "::endgroup::"

echo "::group::Materialize"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:materialize
echo "::endgroup::"

echo "::group::Multidb"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:multidb
echo "::endgroup::"

echo "::group::Prestodb"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:prestodb
echo "::endgroup::"

echo "::group::Trino"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:trino
echo "::endgroup::"

echo "::group::MS SQL"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:mssql
echo "::endgroup::"
