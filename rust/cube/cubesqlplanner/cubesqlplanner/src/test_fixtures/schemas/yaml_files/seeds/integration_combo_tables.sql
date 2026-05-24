DROP TABLE IF EXISTS returns CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customer_orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL
);

CREATE TABLE customer_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    amount NUMERIC(10, 2) NOT NULL
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
    refund_amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO customers (id, name, city) VALUES
    (1, 'Alice', 'New York'),
    (2, 'Bob', 'Boston'),
    (3, 'Charlie', 'New York'),
    (4, 'Diana', 'Boston');

-- totalSpend: Alice=130, Bob=40, Charlie=200, Diana=NULL
INSERT INTO customer_orders (id, customer_id, amount) VALUES
    (1, 1, 80.00),
    (2, 1, 50.00),
    (3, 2, 40.00),
    (4, 3, 200.00);

INSERT INTO orders (id, customer_id, status, amount, created_at) VALUES
    (1, 1, 'completed', 100.00, '2025-03-01 10:00:00'),
    (2, 1, 'completed', 200.00, '2025-03-15 11:00:00'),
    (3, 2, 'pending',    50.00, '2025-03-10 09:00:00'),
    (4, 3, 'completed', 500.00, '2025-04-01 14:00:00'),
    (5, 4, 'completed', 100.00, '2025-03-20 08:00:00'),
    (6, 1, 'pending',    75.00, '2025-04-05 11:00:00');

INSERT INTO returns (id, customer_id, refund_amount, created_at) VALUES
    (1, 1, 50.00, '2025-03-10 10:00:00'),
    (2, 2, 30.00, '2025-03-20 09:00:00'),
    (3, 4, 20.00, '2025-04-01 14:00:00');
