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
    -- January 2024 (with gaps: no orders Jan 5-7, Jan 19-24)
    (1,  1, 'completed',  'electronics', 150.00, '2024-01-02 10:00:00'),
    (2,  2, 'pending',    'books',        45.00, '2024-01-03 11:00:00'),
    (3,  1, 'completed',  'clothing',     75.00, '2024-01-04 09:00:00'),
    (4,  3, 'cancelled',  'electronics', 200.00, '2024-01-08 14:00:00'),
    (5,  2, 'completed',  'books',        30.00, '2024-01-10 08:00:00'),
    (6,  1, 'completed',  'electronics', 350.00, '2024-01-12 16:00:00'),
    (7,  3, 'pending',    'clothing',     90.00, '2024-01-14 10:00:00'),
    (8,  2, 'completed',  'books',        60.00, '2024-01-15 12:00:00'),
    (9,  1, 'cancelled',  'electronics', 120.00, '2024-01-16 09:00:00'),
    (10, 3, 'completed',  'clothing',    180.00, '2024-01-18 15:00:00'),
    -- Gap: no orders Jan 19-24
    (11, 2, 'completed',  'electronics', 250.00, '2024-01-25 11:00:00'),
    -- February 2024
    (12, 1, 'pending',    'books',        40.00, '2024-02-01 10:00:00'),
    (13, 3, 'completed',  'clothing',    110.00, '2024-02-05 14:00:00'),
    (14, 2, 'completed',  'electronics', 300.00, '2024-02-10 09:00:00'),
    (15, 1, 'cancelled',  'books',        55.00, '2024-02-14 11:00:00'),
    (16, 3, 'completed',  'electronics', 220.00, '2024-02-20 16:00:00'),
    -- Gap: no orders Feb 21-28
    -- March 2024
    (17, 2, 'completed',  'clothing',    130.00, '2024-03-01 10:00:00'),
    (18, 1, 'pending',    'electronics',  85.00, '2024-03-05 14:00:00'),
    (19, 3, 'completed',  'books',       170.00, '2024-03-10 09:00:00'),
    (20, 2, 'cancelled',  'clothing',     95.00, '2024-03-15 11:00:00');
