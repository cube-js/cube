#!/bin/bash
exec 2>&1
set -x
set -e

psql -U test -d test -c "
  CREATE TABLE public.events (
      id bigint,
      type character varying(36),
      actor jsonb,
      public boolean,
      created_at timestamp without time zone,
      payload jsonb
  );
"

psql -U test -d test -c "\copy public.events FROM '/data/github-events-2015-01-01.1000.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');"
