#!/bin/sh

cd $1
yarn install
npm install -g cubejs-cli
cubejs deploy

cd ${2:-dashboard-app}
yarn install
yarn build
npm install -g netlify-cli
netlify deploy --dir=${3:-build} --prod