#!/bin/bash
set -e

. .gh-token

echo "Running yarn install..."
yarn install

if [[ -n $(git status --porcelain) ]]; then
  echo "Error: the git working tree is not clean after yarn install. Please review the changes and fix it or commit."
  git status
  exit 1
fi

BUMP=$1
if [ "x$BUMP" == "x" ]; then
  BUMP=patch
fi
yarn lerna version --create-release=github --conventional-commits --force-publish --exact $BUMP
