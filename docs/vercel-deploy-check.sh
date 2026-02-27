#!/bin/sh

# Skip builds from forks
if [ "$VERCEL_GIT_REPO_OWNER" != "cube-js" ]; then
  echo "Skipping deploy for PR from a fork."
  exit 0
fi

# If no previous deployment SHA, always build
if [ -z "$VERCEL_GIT_PREVIOUS_SHA" ]; then
  echo "No previous deployment found, building."
  exit 1
fi

# Check if docs changed since last successful deployment
if git diff --quiet "$VERCEL_GIT_PREVIOUS_SHA" HEAD -- ./; then
  echo "No docs changes since last deployment, skipping."
  exit 0
else
  echo "Docs changes detected, building."
  exit 1
fi
