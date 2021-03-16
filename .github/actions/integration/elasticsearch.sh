#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers

export TEST_ELASTIC_OPENDISTRO_VERSION=1.13.1

echo "::group::ElasticSearch Open Distro ${TEST_ELASTIC_OPENDISTRO_VERSION}";
docker pull amazon/opendistro-for-elasticsearch:${TEST_ELASTIC_OPENDISTRO_VERSION}
yarn lerna run --concurrency 1 --stream --no-prefix integration:elastic
echo "::endgroup::"
