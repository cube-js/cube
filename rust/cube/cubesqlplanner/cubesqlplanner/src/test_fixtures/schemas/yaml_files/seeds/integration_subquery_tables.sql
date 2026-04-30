DROP TABLE IF EXISTS refunds CASCADE;
DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS customer_orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE customer_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    amount NUMERIC(10, 2) NOT NULL
);

CREATE TABLE sales (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    amount NUMERIC(10, 2) NOT NULL,
    category TEXT NOT NULL
);

CREATE TABLE refunds (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    amount NUMERIC(10, 2) NOT NULL
);

INSERT INTO customers (id, name) VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie'),
    (4, 'Diana');

-- totalSpend: Alice=130, Bob=40, Charlie=200, Diana=NULL
INSERT INTO customer_orders (id, customer_id, amount) VALUES
    (1, 1, 80.00),
    (2, 1, 50.00),
    (3, 2, 40.00),
    (4, 3, 200.00);

-- Alice has 2 sales (tests multiplication of Customers.count)
INSERT INTO sales (id, customer_id, amount, category) VALUES
    (1, 1, 300.00, 'online'),
    (2, 2, 150.00, 'retail'),
    (3, 3, 500.00, 'online'),
    (4, 4, 100.00, 'retail'),
    (5, 1, 200.00, 'retail');

-- Refunds: Alice=50, Bob=30, Charlie=NULL, Diana=20
INSERT INTO refunds (id, customer_id, amount) VALUES
    (1, 1, 50.00),
    (2, 2, 30.00),
    (3, 4, 20.00);
