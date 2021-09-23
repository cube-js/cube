-- This script only contains the table creation statements and does not fully represent the table in the database. It's still missing: indices, triggers. Do not use it as a backup.

CREATE SCHEMA staging;

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS staging.orders_id_seq;

-- Table Definition
CREATE TABLE "staging"."orders" (
    "id" int4 NOT NULL DEFAULT nextval('staging.orders_id_seq'::regclass),
    "amount" int4,
    "client_name" text,
    "created_at" timestamptz DEFAULT now(),
    PRIMARY KEY ("id")
);

INSERT INTO "staging"."orders" ("id", "amount", "client_name", "created_at") VALUES
(1, 600, 'A Inc.', '2021-01-01 16:41:16.778819+05'),
(2, 700, 'Imperdiet LLC', '2021-08-10 16:41:16.778819+05'),
(3, 800, 'Urna Ut Tincidunt Inc.', '2021-08-22 16:41:16.778819+05'),
(4, 900, 'Dolor Sit Amet Associates', '2021-09-22 16:41:16.778819+05'),
(5, 1000, 'Non Bibendum Sed Incorporated', '2021-08-30 16:41:16.778819+05');
