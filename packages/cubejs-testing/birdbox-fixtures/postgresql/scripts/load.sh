#!/bin/bash

psql -U test -d test -f /data/*.sql
