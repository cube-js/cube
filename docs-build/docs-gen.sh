#!/bin/bash

echo $NODE_ENV
echo $ALGOLIA_API_KEY
echo $PATH_PREFIX

cd ../docs-gen && yarn && yarn generate