#!/bin/bash
set -e

. .gh-token

BUMP=$1
if [ "x$BUMP" == "x" ]; then
  BUMP=patch
fi

# Cleanup below discards staged as well as unstaged changes to tracked files.
# Refuse to start if the tree already carries any, so that cleanup can only ever
# undo what this script itself did. This gate runs before the trap is installed,
# so a tree it rejects is never touched.
# Untracked files are deliberately not checked: `git restore` cannot touch them,
# and build output that no .gitignore covers must not block a release.
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "Error: working tree has uncommitted changes to tracked files."
  echo "Commit or stash them before releasing - cleanup would discard them."
  echo "If this is leftover state from a failed release run, discard it with:"
  echo "  git restore --staged --worktree ."
  GIT_PAGER=cat git status --short --untracked-files=no
  exit 1
fi

# `set -e` aborts the script before the bump is undone if step 1 or step 2 fails,
# so clean up from an EXIT trap rather than inline: otherwise a failed run leaves
# behind the bumped, partly staged tree that trips lerna's working tree
# validation on the next run. lerna also writes a CHANGELOG.md for any package
# that lacks one, and a newly created one is untracked, so `git restore` alone
# would leave it behind - the pathspecs below match the workspace globs
# ("workspaces" in package.json) and nothing else in the tree.
# INT and TERM are named alongside EXIT so an interrupted run is covered without
# relying on the shell running an EXIT trap for an untrapped signal; the handler
# disarms itself first so its own `exit` cannot run the cleanup a second time,
# then re-raises the signal so an interrupted run does not exit 0.
cleanup_bump() {
  status=$?
  sig=$1
  trap - EXIT INT TERM
  echo "Cleaning up temporary version bump..."
  git restore --staged --worktree . || true
  git clean -fdq -- 'packages/*/CHANGELOG.md' 'rust/*/CHANGELOG.md' || true
  if [ -n "$sig" ]; then
    # Die from the signal now that it is untrapped, so the caller sees the run
    # was interrupted. Exiting with $? instead would report the last completed
    # command's status, which is 0 when the signal lands between commands.
    kill -"$sig" $$
  fi
  exit $status
}
trap cleanup_bump EXIT
trap 'cleanup_bump INT' INT
trap 'cleanup_bump TERM' TERM

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

  exit 1
fi

# The version commit, tag and release are meant to survive, so stop cleaning up.
trap - EXIT INT TERM

echo "Step 4: commit, tag and push version..."
yarn lerna version $BUMP \
  --conventional-commits \
  --force-publish \
  --exact \
  --create-release=github \
  --yes
