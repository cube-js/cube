#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::BigQuery [cloud]"
export CUBEJS_DB_EXPORT_BUCKET=${CUBEJS_DB_BQ_EXPORT_BUCKET}
yarn lerna run --concurrency 1 --stream --no-prefix integration:bigquery
echo "::endgroup::"
