-- This script only contains the table creation statements and does not fully represent the table in the database. It's still missing: indices, triggers. Do not use it as a backup.

CREATE SCHEMA production;

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS production.orders_id_seq;

-- Table Definition
CREATE TABLE "production"."orders" (
    "id" int4 NOT NULL DEFAULT nextval('production.orders_id_seq'::regclass),
    "amount" int4,
    "client_name" text,
    "created_at" timestamptz DEFAULT now(),
    PRIMARY KEY ("id")
);

INSERT INTO "production"."orders" ("id", "amount", "client_name", "created_at") VALUES
(1, 881, 'Sagittis Nullam Industries', '2021-01-01 08:23:16.778819+05'),
(2, 992, 'Egestas A Scelerisque Ltd', '2021-08-10 11:21:14.778819+05'),
(3, 2284, 'Non Company', '2021-08-22 19:20:16.778819+05'),
(4, 8823, 'Quisque Purus Sapien Limited', '2021-09-22 16:16:16.778819+05'),
(5, 27, 'Tortor Inc.', '2021-08-30 22:01:16.778819+05');
