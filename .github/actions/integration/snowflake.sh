#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::Snowflake [cloud]"
export CUBEJS_DB_NAME=DEMO_DB
export CUBEJS_DB_SNOWFLAKE_ACCOUNT=lxb31104
export CUBEJS_DB_SNOWFLAKE_REGION=us-west-2
export CUBEJS_DB_SNOWFLAKE_WAREHOUSE=COMPUTE_WH

yarn lerna run --concurrency 1 --stream --no-prefix smoke:snowflake

echo "::endgroup::"
