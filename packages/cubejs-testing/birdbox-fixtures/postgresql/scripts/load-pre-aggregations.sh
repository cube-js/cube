#!/bin/bash

psql -U test -d test -f /scripts/*.sql
