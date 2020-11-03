#!/bin/bash
set -eo pipefail

cd ../docs-gen && yarn && yarn generate && cd ..

rm -rf .cache
rm -rf public
yarn build --prefix-paths
