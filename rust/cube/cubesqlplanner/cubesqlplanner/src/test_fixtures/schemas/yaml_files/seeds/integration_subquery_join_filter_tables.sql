DROP TABLE IF EXISTS sq_reentry_a CASCADE;
DROP TABLE IF EXISTS sq_reentry_b CASCADE;
DROP TABLE IF EXISTS sq_reentry_c CASCADE;
DROP TABLE IF EXISTS sq_ms_facts CASCADE;
DROP TABLE IF EXISTS sq_ms_lines CASCADE;
DROP TABLE IF EXISTS sq_ms_orders CASCADE;
DROP TABLE IF EXISTS sq_ms_regions CASCADE;
DROP TABLE IF EXISTS sq_purchases CASCADE;
DROP TABLE IF EXISTS sq_users CASCADE;
DROP TABLE IF EXISTS sq_regions CASCADE;
DROP TABLE IF EXISTS sq_channels CASCADE;
DROP TABLE IF EXISTS sq_hub CASCADE;
DROP TABLE IF EXISTS sq_spoke CASCADE;
DROP TABLE IF EXISTS sq_tag CASCADE;
DROP TABLE IF EXISTS sq_shipments CASCADE;
DROP TABLE IF EXISTS sq_customers CASCADE;
DROP TABLE IF EXISTS sq_orders CASCADE;
DROP TABLE IF EXISTS sq_labels CASCADE;

CREATE TABLE sq_orders (
    id INTEGER PRIMARY KEY,
    amount INTEGER NOT NULL
);

CREATE TABLE sq_labels (
    amount INTEGER PRIMARY KEY,
    label TEXT NOT NULL
);

INSERT INTO sq_orders (id, amount) VALUES
    (1, 10),
    (2, 20),
    (3, 30);

INSERT INTO sq_labels (amount, label) VALUES
    (10, 'ten'),
    (20, 'twenty'),
    (40, 'forty');

CREATE TABLE sq_customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE sq_shipments (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    status TEXT NOT NULL
);

INSERT INTO sq_customers (id, name) VALUES
    (1, 'alice'),
    (2, 'bob');

INSERT INTO sq_shipments (id, order_id, customer_id, status) VALUES
    (1, 1, 1, 'sent'),
    (2, 2, 2, 'sent'),
    (3, 3, 1, 'lost');

CREATE TABLE sq_spoke (
    id INTEGER PRIMARY KEY
);

CREATE TABLE sq_tag (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE sq_hub (
    id INTEGER PRIMARY KEY,
    spoke_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    amount INTEGER NOT NULL
);

INSERT INTO sq_spoke (id) VALUES (1), (2);

INSERT INTO sq_tag (id, name) VALUES
    (1, 'a'),
    (2, 'b');

INSERT INTO sq_hub (id, spoke_id, tag_id, kind, amount) VALUES
    (1, 1, 1, 'k1', 10),
    (2, 1, 2, 'k1', 100),
    (3, 2, 1, 'k2', 5);

CREATE TABLE sq_regions (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE sq_channels (
    id INTEGER PRIMARY KEY,
    code INTEGER NOT NULL
);

CREATE TABLE sq_users (
    id INTEGER PRIMARY KEY,
    region_id INTEGER NOT NULL
);

CREATE TABLE sq_purchases (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL
);

INSERT INTO sq_regions (id, name) VALUES
    (1, 'a'),
    (2, 'b');

INSERT INTO sq_channels (id, code) VALUES
    (1, 1),
    (2, 2);

INSERT INTO sq_users (id, region_id) VALUES
    (1, 2),
    (2, 1);

INSERT INTO sq_purchases (id, user_id, region_id, channel_id) VALUES
    (1, 1, 2, 1),
    (2, 1, 2, 2),
    (3, 2, 1, 2);

CREATE TABLE sq_ms_regions (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE sq_ms_orders (id INTEGER PRIMARY KEY, r_id INTEGER NOT NULL);
CREATE TABLE sq_ms_lines (id INTEGER PRIMARY KEY, o_id INTEGER NOT NULL);
CREATE TABLE sq_ms_facts (
    id INTEGER PRIMARY KEY,
    o_id INTEGER NOT NULL,
    m_id INTEGER,
    r_id INTEGER NOT NULL,
    amt INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO sq_ms_regions VALUES (1, 'r'), (2, 's');
INSERT INTO sq_ms_orders VALUES (1, 1), (2, 2);
INSERT INTO sq_ms_facts VALUES
    (1, 1, NULL, 1, 10, '2024-01-01'),
    (2, 1, NULL, 2, 100, '2024-01-01'),
    (3, 2, NULL, 1, 1000, '2024-01-01');

CREATE TABLE sq_reentry_c (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE sq_reentry_a (id INTEGER PRIMARY KEY, c_id INTEGER NOT NULL);
CREATE TABLE sq_reentry_b (id INTEGER PRIMARY KEY, a_id INTEGER NOT NULL, kind TEXT NOT NULL);

INSERT INTO sq_reentry_c VALUES (1, 'x'), (2, 'y');
INSERT INTO sq_reentry_a VALUES (1, 1), (2, 1), (3, 2);
INSERT INTO sq_reentry_b VALUES
    (1, 1, 'k'),
    (2, 1, 'k'),
    (3, 2, 'm'),
    (4, 3, 'k'),
    (5, 3, 'm');
