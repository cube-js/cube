#!/bin/bash
. .gh-token

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$BRANCH" != "master" ]]; then
  echo 'Must be run from the master branch';
  exit 1;
fi

BUMP=$1
if [ "x$BUMP" == "x" ]; then
  BUMP=patch
fi
yarn lerna version --create-release=github --conventional-commits $BUMP
