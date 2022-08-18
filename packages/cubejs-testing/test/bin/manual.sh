#!/usr/bin/env bash
#
# Manually run all tests that are not executed for PR revisions.
# Must be invoked from packages/cubejs-testing/ directory.

set -e
env $(cat ~/.env.athena | xargs) yarn smoke:athena
env $(cat ~/.env.bigquery | xargs) yarn smoke:bigquery
env $(cat ~/.env.redshift | xargs) yarn smoke:redshift
