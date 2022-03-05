#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

echo "::group::Athena [cloud]"
yarn lerna run --concurrency 1 --stream --no-prefix integration:athena
echo "::endgroup::"
