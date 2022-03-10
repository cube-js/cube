#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::Athena [cloud]"
# export CUBEJS_DB_EXPORT_BUCKET=${CUBEJS_DB_ATHENA_EXPORT_BUCKET}
export CUBEJS_DB_EXPORT_BUCKET=s3://cubejs-opensource/testing/export
yarn lerna run --concurrency 1 --stream --no-prefix integration:athena
echo "::endgroup::"
