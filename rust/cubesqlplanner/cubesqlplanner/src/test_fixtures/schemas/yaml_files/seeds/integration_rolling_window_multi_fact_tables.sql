DROP TABLE IF EXISTS mf_returns CASCADE;
DROP TABLE IF EXISTS mf_orders CASCADE;
DROP TABLE IF EXISTS mf_customers CASCADE;

CREATE TABLE mf_customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT
);

CREATE TABLE mf_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES mf_customers(id),
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE mf_returns (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES mf_customers(id),
    reason TEXT NOT NULL,
    refund NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO mf_customers (id, name, city) VALUES
    (1, 'Alice', 'New York'),
    (2, 'Bob', 'San Francisco'),
    (3, 'Charlie', 'Boston');

-- Orders: spread across Jan 2024
INSERT INTO mf_orders (id, customer_id, amount, created_at) VALUES
    (1, 1, 150.00, '2024-01-02 10:00:00'),
    (2, 2,  45.00, '2024-01-05 11:00:00'),
    (3, 3,  75.00, '2024-01-08 09:00:00'),
    (4, 1, 200.00, '2024-01-12 14:00:00'),
    (5, 2,  30.00, '2024-01-15 08:00:00'),
    (6, 3, 350.00, '2024-01-18 16:00:00'),
    (7, 1,  90.00, '2024-01-22 10:00:00'),
    (8, 2,  60.00, '2024-01-25 12:00:00');

-- Returns: different dates than orders, overlapping windows
-- No FK to orders — independent fact table linked only through customers
INSERT INTO mf_returns (id, customer_id, reason, refund, created_at) VALUES
    (1, 1, 'defective',  80.00, '2024-01-03 14:00:00'),
    (2, 2, 'wrong_size', 25.00, '2024-01-06 10:00:00'),
    (3, 3, 'defective',  50.00, '2024-01-10 09:00:00'),
    (4, 1, 'changed_mind', 30.00, '2024-01-13 11:00:00'),
    (5, 2, 'defective', 120.00, '2024-01-16 15:00:00'),
    (6, 3, 'wrong_size', 45.00, '2024-01-20 10:00:00'),
    (7, 1, 'defective',  60.00, '2024-01-24 09:00:00');
