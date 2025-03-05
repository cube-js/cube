#!/bin/bash
. .gh-token

BUMP=$1
if [ "x$BUMP" == "x" ]; then
  BUMP=patch
fi
yarn lerna version --create-release=github --conventional-commits --force-publish --exact $BUMP
