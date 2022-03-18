#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::Athena [cloud]"
export CUBEJS_AWS_REGION=us-east-1
export CUBEJS_AWS_S3_OUTPUT_LOCATION=s3://cubejs-opensource/testing/output
export CUBEJS_DB_EXPORT_BUCKET=s3://cubejs-opensource/testing/export
yarn lerna run --concurrency 1 --stream --no-prefix integration:athena
# yarn lerna run --concurrency 1 --stream --no-prefix birdbox:athena
echo "::endgroup::"
