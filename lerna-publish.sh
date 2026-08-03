#!/bin/bash
set -e

. .gh-token

BUMP=$1
if [ "x$BUMP" == "x" ]; then
  BUMP=patch
fi

echo "Step 1: bumping versions (no commit/push)..."
yarn lerna version $BUMP \
  --conventional-commits \
  --force-publish \
  --exact \
  --no-git-tag-version \
  --no-push \
  --yes

echo "Step 2: doing yarn install check..."
CUBESTORE_SKIP_POST_INSTALL=true yarn install

echo "Step 3: checking git status..."
if git status --porcelain | grep -q '^ M yarn.lock'; then
  echo "Error: yarn.lock is not clean after version bump and yarn install. Please review the changes and fix it or commit."
  echo "If you see any new entries in yarn.lock with @cubejs-*/* packages - probably not all packages versions were updated."
  GIT_PAGER=cat git diff yarn.lock

  echo "Step 4: cleaning up temporary version bump..."
  git restore .

  exit 1
fi

echo "Step 4: cleaning up temporary version bump..."
git restore .

echo "Step 5: commit, tag and push version..."
yarn lerna version $BUMP \
  --conventional-commits \
  --force-publish \
  --exact \
  --create-release=github \
