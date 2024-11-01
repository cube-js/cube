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

# In publish workflow we need to detect branch for tags from github.event.base_ref
# It's possible only when GitHub receives information of tag from separate push
# Lerna uses single push for commit & tag, that's why --no-push is passed and pushed manually
yarn lerna version --create-release=github --conventional-commits --exact --no-push $BUMP
git push -u origin HEAD
# commitdate doesn't work with lerna tags :( version:refname is not an option, because we have LTS branches
git push origin v$(cat lerna.json | jq -r '.version')
