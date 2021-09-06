#!/bin/bash
set -eo pipefail

rm -rf .cache
rm -rf public

yarn && yarn build:netlify
