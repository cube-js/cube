#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers*

export TEST_MONGO_TAG=6.0
export TEST_MONGOBI_VERSION=mongodb-bi-linux-x86_64-ubuntu2204-v2.14.21

echo "::group::MongoBI"
yarn lerna run --concurrency 1 --stream --no-prefix integration:mongobi
echo "::endgroup::"
