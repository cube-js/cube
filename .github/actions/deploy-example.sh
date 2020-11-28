#!/bin/sh

WORKING_DIRECTORY=$1
FRONTEND_SUBDIRECTORY=${2:-dashboard-app}
FRONTEND_DEPLOY_SUBDIRECTORY=${3:-build}

cd $WORKING_DIRECTORY
yarn install
npm install -g cubejs-cli
cubejs deploy

cd $FRONTEND_SUBDIRECTORY
yarn install
yarn build
npm install -g netlify-cli
netlify deploy --dir=$FRONTEND_DEPLOY_SUBDIRECTORY --prod