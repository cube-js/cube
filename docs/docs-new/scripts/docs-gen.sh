#!/bin/bash
# ci
cd ../../docs-gen && NODE_ENV=development yarn install && yarn generate
