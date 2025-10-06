#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::Oracle"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:oracle
echo "::endgroup::"

echo "::group::DuckDB"
# Should we create a separate job integration-duckdb? I believe not, because it works fast.
yarn lerna run --concurrency 1 --stream --no-prefix integration:duckdb
yarn lerna run --concurrency 1 --stream --no-prefix smoke:duckdb
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

#echo "::group::Prestodb"
#docker rm -vf $(docker ps -aq)
#docker rmi -f $(docker images -aq)
#docker pull ahanaio/prestodb-sandbox:0.281
#yarn lerna run --concurrency 1 --stream --no-prefix smoke:prestodb
#echo "::endgroup::"

echo "::group::Trino"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:trino
echo "::endgroup::"

echo "::group::MS SQL"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:mssql
echo "::endgroup::"

echo "::group::MongoBI"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:mongobi
echo "::endgroup::"

# Vertica tests are disabled because around 20.08.2025 someone
# totally removed all vertica-ce docker repository from dockerhub.
# @see https://github.com/vertica/vertica-containers/issues/64
#echo "::group::Vertica"
#yarn lerna run --concurrency 1 --stream --no-prefix smoke:vertica
#echo "::endgroup::"

echo "::group::RBAC"
yarn lerna run --concurrency 1 --stream --no-prefix smoke:rbac
echo "::endgroup::"
