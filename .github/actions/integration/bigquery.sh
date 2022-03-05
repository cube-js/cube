#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::BigQuery [cloud]"
yarn lerna run --concurrency 1 --stream --no-prefix integration:bigquery
echo "::endgroup::"
