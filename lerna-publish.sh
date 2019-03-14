#!/bin/bash
. .gh-token
BUMP=$1
if [ "x$BUMP" == "x" ]; then
  BUMP=patch
fi
lerna publish --github-release --conventional-commits $BUMP