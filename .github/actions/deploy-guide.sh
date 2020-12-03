#!/bin/sh

# Exit on first error
set -e

npm config set loglevel error

# Required environment variable
GUIDE_SLUG=${GUIDE_SLUG}

GUIDE_DIRECTORY=guides/${GUIDE_SLUG}

cd guides/guides-base
yarn install
yarn link
cd ../../

cd "$GUIDE_DIRECTORY"
yarn install
yarn link guides-base
cd ../../

cd guides/guides-base/node_modules
ln -s ../../"${GUIDE_SLUG}"/node_modules/styled-components styled-components
cd ../../../

cd "$GUIDE_DIRECTORY"
yarn build
npm install -g netlify-cli
netlify deploy --dir=public --prod