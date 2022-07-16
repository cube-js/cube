#!/bin/sh

# Exit on first error
set -e

npm config set loglevel error

# Required environment variable
EXAMPLE_SLUG=${EXAMPLE_SLUG}

EXAMPLE_DIRECTORY=examples/${EXAMPLE_SLUG}
EXAMPLE_CUBE_SKIP=${EXAMPLE_CUBE_SKIP:-0}
EXAMPLE_FRONTEND_SKIP=${EXAMPLE_FRONTEND_SKIP:-0}
EXAMPLE_FRONTEND_SUBDIRECTORY=${EXAMPLE_FRONTEND_SUBDIRECTORY:-dashboard-app}
EXAMPLE_FRONTEND_BUILD_SUBDIRECTORY=${EXAMPLE_FRONTEND_BUILD_SUBDIRECTORY:-build}

cd "$EXAMPLE_DIRECTORY"

if [ "$EXAMPLE_CUBE_SKIP" -eq 0 ]
then
  yarn install
  npm install -g cubejs-cli
  cubejs deploy
fi

if [ "$EXAMPLE_FRONTEND_SKIP" -eq 0 ]
then
  cd "$EXAMPLE_FRONTEND_SUBDIRECTORY"
  yarn install
  yarn build
  npm install -g netlify-cli
  netlify deploy --dir="$EXAMPLE_FRONTEND_BUILD_SUBDIRECTORY" --prod
fi