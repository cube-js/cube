#!/bin/bash
set -eo pipefail

rm -rf .cache
rm -rf public
rm -rf dist

yarn && yarn build --prefix-paths && mkdir -p dist/docs && rsync -av --delete public/ dist/docs
