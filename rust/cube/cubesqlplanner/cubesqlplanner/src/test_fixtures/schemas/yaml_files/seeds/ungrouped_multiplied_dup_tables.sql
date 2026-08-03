DROP TABLE IF EXISTS dup_orders CASCADE;
DROP TABLE IF EXISTS dup_customers CASCADE;

CREATE TABLE dup_customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    ltv NUMERIC(10, 2) NOT NULL
);

-- Same name, different keys and different ltv.
INSERT INTO dup_customers (id, name, ltv) VALUES
    (1, 'dup', 100),
    (2, 'dup', 50);

CREATE TABLE dup_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status TEXT NOT NULL
);

INSERT INTO dup_orders (id, customer_id, status) VALUES
    (10, 1, 'new'),
    (11, 2, 'new');
