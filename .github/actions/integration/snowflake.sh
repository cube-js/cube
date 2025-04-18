#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::Snowflake [cloud]"
export CUBEJS_DB_NAME=DEMO_DB
export CUBEJS_DB_SNOWFLAKE_ACCOUNT=lxb31104
export CUBEJS_DB_SNOWFLAKE_REGION=us-west-2
export CUBEJS_DB_SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export CUBEJS_DB_USER=$DRIVERS_TESTS_SNOWFLAKE_CUBEJS_DB_USER
export CUBEJS_DB_PASS=$DRIVERS_TESTS_SNOWFLAKE_CUBEJS_DB_PASS

yarn lerna run --concurrency 1 --stream --no-prefix smoke:snowflake

echo "::endgroup::"
