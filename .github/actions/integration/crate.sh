#!/bin/bash
set -eo pipefail

export DEBUG=testcontainers

echo "::group::Crate"
yarn lerna run --concurrency 1 --stream --no-prefix integration:crate
echo "::endgroup::"
