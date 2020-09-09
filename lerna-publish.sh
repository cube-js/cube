#!/bin/bash
. .gh-token
echo $GH_TOKEN
# BUMP=$1
# if [ "x$BUMP" == "x" ]; then
#   BUMP=patch
# fi
# yarn lerna version --github-release --conventional-commits $BUMP