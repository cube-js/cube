CREATE EXTENSION IF NOT EXISTS hll;

DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    status TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

INSERT INTO customers (id, name, city) VALUES
    (1, 'Alice Johnson', 'New York'),
    (2, 'Bob Smith', 'San Francisco'),
    (3, 'Charlie Brown', NULL),
    (4, 'Alice Cooper', 'New York'),
    (5, 'Diana Prince', 'Boston');

INSERT INTO orders (id, customer_id, status, amount, created_at, updated_at) VALUES
    (1, 1, 'completed', 100.00, '2024-01-15 10:00:00', '2024-01-16 09:00:00'),
    (2, 1, 'completed', 200.00, '2024-01-20 14:00:00', '2024-02-01 10:00:00'),
    (3, 2, 'pending',    50.00, '2024-02-10 09:00:00', '2024-02-11 11:00:00'),
    (4, 2, 'completed', 300.00, '2024-02-15 11:00:00', '2024-03-01 14:00:00'),
    (5, 3, 'cancelled',  25.00, '2024-03-01 16:00:00', '2024-03-02 08:00:00'),
    (6, 4, 'completed', 150.00, '2024-03-10 08:00:00', '2024-03-10 12:00:00'),
    (7, 4, 'pending',    75.00, '2024-03-15 12:00:00', '2024-03-16 09:00:00'),
    (8, 5, 'completed', 500.00, '2024-04-01 10:00:00', '2024-04-02 10:00:00'),
    (9, 3, 'pending',    40.00, '2024-01-15 15:00:00', '2024-01-15 16:00:00');
