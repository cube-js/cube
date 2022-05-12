#!/bin/bash

cat > seed.sql << EOL

CREATE SOURCE hn_raw
FROM PUBNUB
SUBSCRIBE KEY 'sub-c-c00db4fc-a1e7-11e6-8bfd-0619f8945a4f'
CHANNEL 'hacker-news';

CREATE VIEW hn AS
SELECT
  (item::jsonb)->>'link' AS link,
  (item::jsonb)->>'comments' AS comments,
  (item::jsonb)->>'title' AS title,
  ((item::jsonb)->>'rank')::int AS rank
FROM (
  SELECT jsonb_array_elements(text::jsonb) AS item
  FROM hn_raw
);

CREATE MATERIALIZED VIEW hn_top AS
SELECT link, comments, title, MIN(rank) AS rank
FROM hn
GROUP BY 1, 2, 3;

EOL

psql -U materialize -h materialize -p 6875 materialize -f ./seed.sql
