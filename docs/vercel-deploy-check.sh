#!/bin/sh

if [ "$VERCEL_GIT_REPO_OWNER" != "cube-js" ]; then
  echo "Skipping deploy for PR from a fork."
  exit 0
fi

# In other case return the usual git diff for a root folder
git diff HEAD^ HEAD --quiet ./
