#!/bin/bash
set -eo pipefail

cd ../docs-gen && yarn && yarn generate && cd ../docs-build

rm -rf .cache
rm -rf public

yarn && yarn build --prefix-paths