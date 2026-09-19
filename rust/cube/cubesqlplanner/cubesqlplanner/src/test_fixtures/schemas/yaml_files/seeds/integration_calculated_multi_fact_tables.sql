DROP TABLE IF EXISTS payment_tags CASCADE;
DROP TABLE IF EXISTS payment_meta CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS rates CASCADE;

CREATE TABLE customers (
    id TEXT PRIMARY KEY,
    tier TEXT NOT NULL
);

CREATE TABLE rates (
    currency TEXT PRIMARY KEY,
    fx_rate NUMERIC(10, 4) NOT NULL
);

CREATE TABLE merchants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    commission NUMERIC(10, 4) NOT NULL
);

-- `id` is TEXT on purpose: it is the operand that ends up in arithmetic
-- when a calculated measure loses the aggregation around its components.
CREATE TABLE payments (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    currency TEXT NOT NULL REFERENCES rates(currency),
    merchant_id TEXT NOT NULL REFERENCES merchants(id),
    -- nullable: p3 has no customer, so a tree rooted at `customers` cannot
    -- reach it
    customer_id TEXT REFERENCES customers(id),
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE payment_meta (
    id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL REFERENCES payments(id),
    value TEXT NOT NULL
);

CREATE TABLE payment_tags (
    id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL REFERENCES payments(id),
    tag TEXT NOT NULL
);

INSERT INTO rates (currency, fx_rate) VALUES
    ('EUR', 1.0),
    ('USD', 2.0);

INSERT INTO merchants (id, name, commission) VALUES
    ('mer1', 'Acme',   0.1),
    ('mer2', 'Globex', 0.5);

INSERT INTO customers (id, tier) VALUES
    ('c1', 'gold'),
    ('c2', 'silver');

INSERT INTO payments (id, status, amount, currency, merchant_id, customer_id, created_at) VALUES
    ('p1', 'SUCCESS',  100.00, 'EUR', 'mer1', 'c1',  '2025-01-01 00:00:00'),
    ('p2', 'SUCCESS',  200.00, 'EUR', 'mer1', 'c1',  '2025-01-02 00:00:00'),
    ('p3', 'DECLINED', 300.00, 'EUR', 'mer2', NULL,  '2025-01-03 00:00:00'),
    ('p4', 'SUCCESS',  400.00, 'USD', 'mer2', 'c2',  '2025-01-04 00:00:00'),
    -- sole payment under meta value 'C', and it has no customer: a tree rooted
    -- at `customers` cannot reach it, so that leg reports no 'C' at all
    ('p5', 'SUCCESS',  500.00, 'EUR', 'mer1', NULL,  '2025-01-05 00:00:00');

-- p1 carries two meta rows so grouping by `payment_meta.value` multiplies it.
INSERT INTO payment_meta (id, payment_id, value) VALUES
    ('m1',  'p1', 'A'),
    ('m1b', 'p1', 'A'),
    ('m2',  'p2', 'A'),
    ('m3',  'p3', 'A'),
    ('m4',  'p4', 'B'),
    ('m5',  'p5', 'C');

-- p1 also carries two tags, so pulling both branches into one tree squares
-- its rows.
INSERT INTO payment_tags (id, payment_id, tag) VALUES
    ('t1', 'p1', 'vip'),
    ('t2', 'p1', 'new'),
    ('t3', 'p2', 'vip'),
    ('t4', 'p4', 'new');
