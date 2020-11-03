#!/bin/bash
set -eo pipefail

cd ../docs-gen && yarn && yarn generate