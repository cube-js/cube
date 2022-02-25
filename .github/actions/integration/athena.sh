#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::Athena [cloud]"
CUBEJS_TEST_ENV=${TODO}/.env yarn lerna run --concurrency 1 --stream --no-prefix integration:athena
echo "::endgroup::"
