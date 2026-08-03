#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_VERTICA_VERSION=12.0.4-0

echo "::group::Vertica ${TEST_VERTICA_VERSION}"
docker pull vertica/vertica-ce:${TEST_VERTICA_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:vertica
echo "::endgroup::"
