DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS returns CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL
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

INSERT INTO customers (id, name, city) VALUES
    (1, 'Alice', 'New York'),
    (2, 'Bob', 'Boston'),
    (3, 'Charlie', 'Chicago'),
    (4, 'Diana', 'New York');

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
