#!/bin/bash
set -eo pipefail

# Debug log for test containers
export DEBUG=testcontainers*

export TEST_VERTICA_VERSION=12.0.4-0

echo "::group::Vertica ${TEST_VERTICA_VERSION}"
docker info
echo "Before pull" && df -h
docker pull vertica/vertica-ce:${TEST_VERTICA_VERSION}
echo "After pull" && df -h
bash -c 'sleep 5 && echo "5 sec after pull" && df -h' &
bash -c 'sleep 30 && echo "30 sec after pull" && df -h' &
bash -c 'sleep 45 && echo "45 sec after pull" && df -h' &
yarn lerna run --concurrency 1 --stream --no-prefix integration:vertica
echo "After tests" && df -h
echo "::endgroup::"
