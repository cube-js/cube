DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS returns CASCADE;
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
    amount NUMERIC(10, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE returns (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    reason TEXT NOT NULL,
    refund_amount NUMERIC(10, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO customers (id, name, city) VALUES
    (1, 'Alice', 'New York'),
    (2, 'Bob', 'Boston'),
    (3, 'Charlie', 'Chicago');

-- Alice: 3 orders, Bob: 1 order, Charlie: 0 orders
INSERT INTO orders (id, customer_id, status, amount, created_at) VALUES
    (1, 1, 'completed', 100.00, '2025-03-01 10:00:00'),
    (2, 1, 'completed', 200.00, '2025-03-02 11:00:00'),
    (3, 1, 'pending', 50.00, '2025-03-03 09:00:00'),
    (4, 2, 'completed', 300.00, '2025-03-04 14:00:00');

-- Alice: 1 return, Bob: 0 returns, Charlie: 2 returns
INSERT INTO returns (id, customer_id, reason, refund_amount, created_at) VALUES
    (1, 1, 'defective', 100.00, '2025-03-05 10:00:00'),
    (2, 3, 'wrong_item', 75.00, '2025-03-06 11:00:00'),
    (3, 3, 'not_needed', 25.00, '2025-03-07 12:00:00');
