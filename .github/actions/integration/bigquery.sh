#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::BigQuery [cloud]"
export CUBEJS_DB_BQ_PROJECT_ID=cube-open-source
export CUBEJS_DB_EXPORT_BUCKET=cube-open-source-export-bucket
yarn lerna run --concurrency 1 --stream --no-prefix integration:bigquery
# yarn lerna run --concurrency 1 --stream --no-prefix birdbox:bigquery
echo "::endgroup::"
