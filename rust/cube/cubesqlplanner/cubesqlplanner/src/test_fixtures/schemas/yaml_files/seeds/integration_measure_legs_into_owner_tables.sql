CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    city TEXT NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL
);

CREATE TABLE payments (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    amount NUMERIC(10, 2) NOT NULL
);

CREATE TABLE shipments (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL
);

-- Chicago has no orders.
INSERT INTO customers (id, city) VALUES
    (1, 'New York'),
    (2, 'Boston'),
    (3, 'Chicago');

-- Order 4 has neither payments nor shipments.
INSERT INTO orders (id, customer_id, status, amount) VALUES
    (1, 1, 'completed', 100.00),
    (2, 1, 'pending',   200.00),
    (3, 2, 'completed', 300.00),
    (4, 2, 'completed',  50.00);

-- Order 1 is paid in two parts; payment 5 belongs to no order.
INSERT INTO payments (id, order_id, amount) VALUES
    (1, 1,     60.00),
    (2, 1,     40.00),
    (3, 2,    150.00),
    (4, 3,    300.00),
    (5, NULL,   9.00);

-- Order 2 ships in two parts.
INSERT INTO shipments (id, order_id) VALUES
    (1, 1),
    (2, 2),
    (3, 2),
    (4, 3);
