#!/bin/bash
set -e

. .gh-token

BUMP=$1
if [ "x$BUMP" == "x" ]; then
  BUMP=patch
fi

# Step 4 cleans up with `git restore --staged --worktree .`, which discards both
# staged and unstaged changes to tracked files. Refuse to start if the tree
# already carries any, so that cleanup can only ever undo what this script did.
# Untracked files are deliberately not checked: `git restore` cannot touch them,
# and build output that no .gitignore covers must not block a release.
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "Error: working tree has uncommitted changes to tracked files."
  echo "Commit or stash them before releasing - step 4 cleanup would discard them."
  GIT_PAGER=cat git status --short --untracked-files=no
  exit 1
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
  git restore --staged --worktree .
  git clean -fdq -- '*CHANGELOG.md'

  exit 1
fi

echo "Step 4: cleaning up temporary version bump..."
git restore --staged --worktree .
git clean -fdq -- '*CHANGELOG.md'

echo "Step 5: commit, tag and push version..."
yarn lerna version $BUMP \
  --conventional-commits \
  --force-publish \
  --exact \
  --create-release=github \
  --yes
