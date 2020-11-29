#!/bin/sh

FRONTEND_SUBDIRECTORY=${FRONTEND_SUBDIRECTORY:-dashboard-app}
FRONTEND_DEPLOY_SUBDIRECTORY=${FRONTEND_DEPLOY_SUBDIRECTORY:-build}

npm config set loglevel warn

yarn install
npm install -g cubejs-cli
cubejs deploy

cd $FRONTEND_SUBDIRECTORY
yarn install
yarn build
npm install -g netlify-cli
netlify deploy --dir=$FRONTEND_DEPLOY_SUBDIRECTORY --prod