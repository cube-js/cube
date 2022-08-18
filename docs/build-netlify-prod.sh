#!/bin/bash
set -eo pipefail

rm -rf .cache
rm -rf public
rm -rf dist

yarn && yarn build --prefix-paths && mkdir -p dist/docs && mv public/_headers public/_redirects dist/ && rsync -av --delete public/ dist/docs
