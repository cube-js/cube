DROP TABLE IF EXISTS msq_orders CASCADE;
DROP TABLE IF EXISTS msq_customers CASCADE;
DROP TABLE IF EXISTS msq_regions CASCADE;

CREATE TABLE msq_regions (
    id INTEGER PRIMARY KEY,
    weight INTEGER NOT NULL
);

CREATE TABLE msq_customers (
    id INTEGER PRIMARY KEY,
    region_id INTEGER NOT NULL,
    lifetime_value INTEGER NOT NULL
);

CREATE TABLE msq_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    category TEXT NOT NULL
);

INSERT INTO msq_regions (id, weight) VALUES
    (1, 10),
    (2, 100);

-- Lifetime values are distinct powers of ten, so every sum names the
-- customers it adds up and a customer counted twice shows as a 2 digit.
-- Customer 4 has no orders and must never appear under an `orders` dimension.
INSERT INTO msq_customers (id, region_id, lifetime_value) VALUES
    (1, 1, 100),
    (2, 2, 10),
    (3, 1, 1),
    (4, 2, 1000);

-- Customer 1 has two completed orders: a lost dedup counts it twice under
-- `completed` (201 instead of 101). Its orders differ in category, so
-- under `by_category` it lands once in each.
INSERT INTO msq_orders (id, customer_id, status, category) VALUES
    (1, 1, 'completed', 'books'),
    (2, 1, 'completed', 'games'),
    (3, 2, 'pending', 'books'),
    (4, 3, 'completed', 'books');
