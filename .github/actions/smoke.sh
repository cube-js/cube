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