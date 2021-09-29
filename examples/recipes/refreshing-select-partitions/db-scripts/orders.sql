-- -------------------------------------------------------------
-- TablePlus 4.2.0(388)
--
-- https://tableplus.com/
--
-- Database: refreshpartitions
-- Generation Time: 2021-09-27 14:30:31.9760
-- -------------------------------------------------------------


DROP TABLE IF EXISTS "public"."orders";
-- This script only contains the table creation statements and does not fully represent the table in the database. It's still missing: indices, triggers. Do not use it as a backup.

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS orders_id_seq;

-- Table Definition
CREATE TABLE "public"."orders" (
    "id" int4 NOT NULL DEFAULT nextval('orders_id_seq'::regclass),
    "number" text,
    "status" text,
    "created_at" timestamp NOT NULL DEFAULT now(),
    "updated_at" timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY ("id")
);

INSERT INTO "public"."orders" ("id", "number", "status", "created_at", "updated_at") VALUES
(1, '1', 'processing', '2021-08-10 14:26:40.387848', '2021-08-10 14:26:40.387848'),
(2, '2', 'completed', '2021-08-20 13:21:38.773825', '2021-08-20 13:21:38.773825'),
(3, '3', 'shipped', '2021-09-01 10:27:38.773825', '2021-09-01 10:27:38.773825'),
(4, '4', 'completed', '2021-09-20 10:27:38.773825', '2021-09-20 10:27:38.773825'),
(5, '5', 'processing', '2021-09-26 10:27:38.773825', '2021-09-26 10:27:38.773825');
