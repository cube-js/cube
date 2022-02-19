#!/bin/bash

set -x
set -e

psql -c "
  CREATE DATABASE test WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE = 'en_US.utf8';
"

psql -c "
  ALTER DATABASE test OWNER TO test;
"

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

psql -U test -d test -c "
  COPY public.events(id, type, actor, public, created_at, payload)
  FROM '/data/github-events-2015-01-01.1000.csv'
  DELIMITER ','
  CSV HEADER;
"
