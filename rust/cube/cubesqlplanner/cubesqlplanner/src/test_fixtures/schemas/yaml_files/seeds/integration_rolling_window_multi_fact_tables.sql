DROP TABLE IF EXISTS mf_messages CASCADE;
DROP TABLE IF EXISTS mf_payments CASCADE;
DROP TABLE IF EXISTS mf_customers CASCADE;

CREATE TABLE mf_customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT,
    lifetime_value NUMERIC(10, 2) NOT NULL,
    registered_at TIMESTAMP NOT NULL
);

CREATE TABLE mf_payments (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES mf_customers(id),
    amount NUMERIC(10, 2) NOT NULL,
    payment_type TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE mf_messages (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES mf_customers(id),
    created_at TIMESTAMP NOT NULL
);

-- Customers registered on different days in January 2024
INSERT INTO mf_customers (id, name, city, lifetime_value, registered_at) VALUES
    (1, 'Alice', 'New York', 500.00, '2024-01-10 10:00:00'),
    (2, 'Bob', 'San Francisco', 300.00, '2024-01-15 12:00:00'),
    (3, 'Charlie', 'Boston', 200.00, '2024-01-20 09:00:00');

-- Payments: each customer pays at various times after registration
INSERT INTO mf_payments (id, customer_id, amount, payment_type, created_at) VALUES
    (1, 1, 100.00, 'card', '2024-01-11 10:00:00'),
    (2, 1, 200.00, 'cash', '2024-01-18 14:00:00'),
    (3, 2, 150.00, 'card', '2024-01-16 11:00:00'),
    (4, 2,  50.00, 'card', '2024-01-22 09:00:00'),
    (5, 3,  75.00, 'cash', '2024-01-21 16:00:00'),
    (6, 3, 125.00, 'card', '2024-01-24 10:00:00');

-- Messages: support messages from each customer
INSERT INTO mf_messages (id, customer_id, created_at) VALUES
    (1, 1, '2024-01-12 09:00:00'),
    (2, 1, '2024-01-14 11:00:00'),
    (3, 2, '2024-01-16 10:00:00'),
    (4, 2, '2024-01-18 14:00:00'),
    (5, 2, '2024-01-22 08:00:00'),
    (6, 3, '2024-01-21 12:00:00');
