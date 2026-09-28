DROP TABLE IF EXISTS addresses CASCADE;
DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS returns CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS regions CASCADE;

CREATE TABLE regions (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    region_id INTEGER NOT NULL REFERENCES regions(id),
    lifetime_value NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    status TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE returns (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    reason TEXT NOT NULL,
    refund_amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE reviews (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    rating INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE addresses (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    street TEXT NOT NULL
);

INSERT INTO regions (id, name) VALUES
    (1, 'East'),
    (2, 'Midwest');

INSERT INTO customers (id, name, city, region_id, lifetime_value, created_at) VALUES
    (1, 'Alice', 'New York', 1, 1000.00, '2025-01-15 10:00:00'),
    (2, 'Bob', 'Boston', 1, 2000.00, '2025-02-01 12:00:00'),
    (3, 'Charlie', 'Chicago', 2, 500.00, '2025-02-15 09:00:00'),
    (4, 'Diana', 'New York', 1, 1500.00, '2025-03-01 08:00:00');

INSERT INTO orders (id, customer_id, status, amount, created_at) VALUES
    (1, 1, 'completed', 100.00, '2025-03-01 10:00:00'),
    (2, 1, 'completed', 200.00, '2025-03-02 11:00:00'),
    (3, 1, 'pending',    50.00, '2025-03-03 09:00:00'),
    (4, 2, 'completed', 300.00, '2025-03-04 14:00:00'),
    (5, 2, 'pending',   100.00, '2025-03-05 10:00:00'),
    (6, 4, 'completed', 400.00, '2025-03-06 08:00:00'),
    (7, 1, 'pending',    75.00, '2025-03-07 11:00:00'),
    (8, 2, 'completed', 150.00, '2025-03-08 09:00:00');

INSERT INTO returns (id, customer_id, reason, refund_amount, created_at) VALUES
    (1, 1, 'defective',  100.00, '2025-03-05 10:00:00'),
    (2, 3, 'wrong_item',  75.00, '2025-03-06 11:00:00'),
    (3, 3, 'not_needed',  25.00, '2025-03-07 12:00:00'),
    (4, 2, 'defective',   50.00, '2025-03-08 09:00:00'),
    (5, 2, 'wrong_item',  80.00, '2025-03-09 14:00:00');

INSERT INTO reviews (id, customer_id, rating, created_at) VALUES
    (1, 1, 5, '2025-03-10 10:00:00'),
    (2, 1, 4, '2025-03-11 11:00:00'),
    (3, 4, 3, '2025-03-12 09:00:00'),
    (4, 3, 5, '2025-03-13 14:00:00');

INSERT INTO addresses (id, customer_id, street) VALUES
    (1, 1, 'Main St'),
    (2, 1, 'Oak Ave'),
    (3, 2, 'Elm St'),
    (4, 3, 'Pine Rd'),
    (5, 4, 'Maple Ln'),
    (6, 4, 'Birch Way');
