DROP TABLE IF EXISTS mss_orders CASCADE;
DROP TABLE IF EXISTS mss_customers CASCADE;

CREATE TABLE mss_customers (
    id INTEGER PRIMARY KEY
);

INSERT INTO mss_customers (id) VALUES (1), (2), (3);

CREATE TABLE mss_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES mss_customers(id),
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

-- Customer 1 orders every month; customers 2 and 3 order in one month only, so
-- the month after it has no monthly_spend for them.
INSERT INTO mss_orders (id, customer_id, amount, created_at) VALUES
    (1, 1, 50, '2024-01-10'),
    (2, 1, 30, '2024-02-10'),
    (3, 1, 15, '2024-03-10'),
    (4, 2, 20, '2024-02-15'),
    (5, 3, 100, '2024-01-20');
