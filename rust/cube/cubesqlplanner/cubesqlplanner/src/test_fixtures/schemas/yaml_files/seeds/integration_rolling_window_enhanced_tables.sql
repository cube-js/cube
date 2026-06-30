DROP TABLE IF EXISTS rw_orders CASCADE;
DROP TABLE IF EXISTS rw_customers CASCADE;

CREATE TABLE rw_customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT
);

CREATE TABLE rw_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES rw_customers(id),
    status TEXT NOT NULL,
    category TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO rw_customers (id, name, city) VALUES
    (1, 'Alice Johnson', 'New York'),
    (2, 'Bob Smith', 'San Francisco'),
    (3, 'Charlie Brown', 'Boston');

INSERT INTO rw_orders (id, customer_id, status, category, amount, created_at) VALUES
    -- January 2024
    (1,  1, 'completed',  'electronics', 150.00, '2024-01-02 10:00:00'),
    (2,  2, 'pending',    'books',        45.00, '2024-01-05 11:00:00'),
    (3,  3, 'completed',  'clothing',     75.00, '2024-01-08 09:00:00'),
    -- Jan 12: two orders, two customers
    (4,  1, 'completed',  'electronics', 200.00, '2024-01-12 10:00:00'),
    (5,  2, 'cancelled',  'books',        30.00, '2024-01-12 14:00:00'),
    (6,  3, 'completed',  'clothing',    350.00, '2024-01-15 12:00:00'),
    (7,  1, 'pending',    'electronics',  90.00, '2024-01-18 09:00:00'),
    -- End of January cluster
    (8,  2, 'completed',  'books',        60.00, '2024-01-26 11:00:00'),
    (9,  1, 'completed',  'electronics', 120.00, '2024-01-28 10:00:00'),
    (10, 3, 'cancelled',  'clothing',    180.00, '2024-01-30 14:00:00'),
    -- February 2024
    (11, 2, 'completed',  'electronics', 250.00, '2024-02-03 09:00:00'),
    (12, 1, 'pending',    'books',        40.00, '2024-02-10 11:00:00'),
    -- Feb 15: three orders, three customers
    (13, 1, 'completed',  'electronics', 110.00, '2024-02-15 10:00:00'),
    (14, 2, 'cancelled',  'clothing',    300.00, '2024-02-15 12:00:00'),
    (15, 3, 'completed',  'books',        55.00, '2024-02-15 14:00:00'),
    -- End of February cluster
    (16, 3, 'completed',  'electronics', 220.00, '2024-02-25 10:00:00'),
    (17, 1, 'completed',  'clothing',    130.00, '2024-02-27 16:00:00'),
    -- March 2024
    (18, 2, 'pending',    'books',        85.00, '2024-03-05 10:00:00'),
    (19, 3, 'completed',  'electronics', 170.00, '2024-03-12 09:00:00'),
    -- End of March/Q1 cluster
    (20, 1, 'completed',  'clothing',     95.00, '2024-03-26 11:00:00'),
    (21, 2, 'completed',  'books',       160.00, '2024-03-28 14:00:00'),
    (22, 3, 'cancelled',  'electronics', 140.00, '2024-03-30 10:00:00');
