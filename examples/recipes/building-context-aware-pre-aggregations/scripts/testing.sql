-- This script only contains the table creation statements and does not fully represent the table in the database. It's still missing: indices, triggers. Do not use it as a backup.

CREATE SCHEMA testing;

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS testing.orders_id_seq;

-- Table Definition
CREATE TABLE "testing"."orders" (
    "id" int4 NOT NULL DEFAULT nextval('testing.orders_id_seq'::regclass),
    "amount" int4,
    "client_name" text,
    "created_at" timestamptz DEFAULT now(),
    PRIMARY KEY ("id")
);

INSERT INTO "testing"."orders" ("id", "amount", "client_name", "created_at") VALUES
(1, 100, 'Ullamcorper Duis LLC', '2021-09-22 16:41:16.778819+05'),
(2, 200, 'Ipsum Incorporated', '2021-09-22 16:41:16.778819+05'),
(3, 300, 'Enim Ltd', '2021-09-22 16:41:16.778819+05'),
(4, 400, 'Ipsum Leo Foundation', '2021-09-22 16:41:16.778819+05'),
(5, 500, 'At Pede Cras Corporation', '2021-09-22 16:41:16.778819+05');
