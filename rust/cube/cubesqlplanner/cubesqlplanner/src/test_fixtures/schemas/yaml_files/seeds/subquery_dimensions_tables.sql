DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS customer_orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id INTEGER PRIMARY KEY
);

CREATE TABLE customer_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    amount NUMERIC(10, 2) NOT NULL
);

CREATE TABLE sales (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    amount NUMERIC(10, 2) NOT NULL
);

INSERT INTO customers (id) VALUES (1), (2), (3), (4);

-- Customer 1: totalSpend = 80 + 50 = 130 (> 100)
-- Customer 2: totalSpend = 40 (≤ 100)
-- Customer 3: totalSpend = 200 (> 100)
-- Customer 4: no orders → totalSpend = NULL
INSERT INTO customer_orders (id, customer_id, amount) VALUES
    (1, 1, 80.00),
    (2, 1, 50.00),
    (3, 2, 40.00),
    (4, 3, 200.00);

-- Sales are independent from customer_orders
-- Customer 1: 300
-- Customer 2: 150
-- Customer 3: 500
-- Customer 4: 100
INSERT INTO sales (id, customer_id, amount) VALUES
    (1, 1, 300.00),
    (2, 2, 150.00),
    (3, 3, 500.00),
    (4, 4, 100.00);
