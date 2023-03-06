#!/bin/bash
exec 2>&1
set -x
set -e

psql -U test -d test -f /data/*.sql
