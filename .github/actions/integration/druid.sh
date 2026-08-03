#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_POSTGRES_VERSION=13
export TEST_ZOOKEEPER_VERSION=3.5
export TEST_DRUID_VERSION=27.0.0

echo "::group::Druid ${TEST_DRUID_VERSION}";

docker pull postgres:${TEST_POSTGRES_VERSION}
docker pull zookeeper:${TEST_ZOOKEEPER_VERSION}
docker pull apache/druid:${TEST_DRUID_VERSION}

echo "Druid ${TEST_DRUID_VERSION}";
yarn lerna run --concurrency 1 --stream --no-prefix integration:druid
echo "::endgroup::"
