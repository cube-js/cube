CREATE TABLE regions (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    city TEXT NOT NULL,
    region_id INTEGER NOT NULL,
    lifetime_value NUMERIC(10, 2) NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    fee NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE returns (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    reason TEXT NOT NULL,
    refund NUMERIC(10, 2) NOT NULL
);

CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL
);

INSERT INTO regions (id, name) VALUES
    (1, 'East'),
    (2, 'Midwest');

-- Chicago has no orders; Diana's order is refunded in full.
INSERT INTO customers (id, city, region_id, lifetime_value) VALUES
    (1, 'New York', 1, 1000.00),
    (2, 'Boston', 1, 2000.00),
    (3, 'Chicago', 2, 500.00),
    (4, 'New York', 1, 1500.00);

INSERT INTO orders (id, customer_id, status, amount, fee, created_at) VALUES
    (1, 1, 'completed', 100.00, 10.00, '2025-01-05 10:00:00'),
    (2, 1, 'completed', 200.00, 20.00, '2025-01-20 10:00:00'),
    (3, 1, 'pending',    40.00,  4.00, '2025-02-03 10:00:00'),
    (4, 2, 'completed', 300.00, 30.00, '2025-02-10 10:00:00'),
    (5, 2, 'pending',  1000.00, 100.00, '2025-02-15 10:00:00'),
    (6, 4, 'completed', 400.00, 40.00, '2025-03-01 10:00:00');

-- Order 1 has two returns; return 4 belongs to no order.
INSERT INTO returns (id, order_id, reason, refund) VALUES
    (1, 1,    'defective',   30.00),
    (2, 1,    'wrong_item',  20.00),
    (3, 4,    'defective',   50.00),
    (4, NULL, 'lost',         7.00),
    (5, 6,    'wrong_item', 400.00);

-- Orders 3 and 6 have no items.
INSERT INTO items (id, order_id, sku, qty) VALUES
    (1, 1, 'A', 1),
    (2, 1, 'B', 2),
    (3, 2, 'A', 3),
    (4, 4, 'C', 1),
    (5, 5, 'A', 1),
    (6, 5, 'B', 1),
    (7, 5, 'C', 1);
