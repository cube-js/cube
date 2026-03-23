DROP TABLE IF EXISTS ms_returns CASCADE;
DROP TABLE IF EXISTS ms_orders CASCADE;
DROP TABLE IF EXISTS ms_customers CASCADE;

CREATE TABLE ms_customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL
);

INSERT INTO ms_customers (id, name, city) VALUES
    (1, 'Alice', 'New York'),
    (2, 'Bob', 'London'),
    (3, 'Charlie', 'Berlin');

CREATE TABLE ms_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES ms_customers(id),
    status TEXT NOT NULL,
    category TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

-- Monthly totals: Jan=500, Feb=750, Mar=1000, Grand=2250
-- Status totals:  completed=1400, pending=650, cancelled=200
-- Category totals: electronics=650, books=880, clothing=720
-- Customer totals: Alice(1)=500, Bob(2)=750, Charlie(3)=1000
INSERT INTO ms_orders (id, customer_id, status, category, amount, created_at) VALUES
    -- January 2024: total=500 (completed=300, pending=150, cancelled=50) — customer 1 (Alice)
    (1,  1, 'completed',  'electronics', 100.00, '2024-01-05 10:00:00'),
    (2,  1, 'completed',  'books',       200.00, '2024-01-10 11:00:00'),
    (3,  1, 'pending',    'clothing',    120.00, '2024-01-15 09:00:00'),
    (4,  1, 'cancelled',  'electronics',  50.00, '2024-01-20 14:00:00'),
    (5,  1, 'pending',    'books',        30.00, '2024-01-25 08:00:00'),
    -- February 2024: total=750 (completed=500, pending=200, cancelled=50) — customer 2 (Bob)
    (6,  2, 'completed',  'clothing',    300.00, '2024-02-03 10:00:00'),
    (7,  2, 'completed',  'electronics', 200.00, '2024-02-08 16:00:00'),
    (8,  2, 'pending',    'books',       150.00, '2024-02-14 12:00:00'),
    (9,  2, 'cancelled',  'clothing',     50.00, '2024-02-19 09:00:00'),
    (10, 2, 'pending',    'electronics',  50.00, '2024-02-25 15:00:00'),
    -- March 2024: total=1000 (completed=600, pending=300, cancelled=100) — customer 3 (Charlie)
    (11, 3, 'completed',  'books',       400.00, '2024-03-02 10:00:00'),
    (12, 3, 'completed',  'electronics', 200.00, '2024-03-07 14:00:00'),
    (13, 3, 'pending',    'clothing',    250.00, '2024-03-12 09:00:00'),
    (14, 3, 'cancelled',  'books',       100.00, '2024-03-18 11:00:00'),
    (15, 3, 'pending',    'electronics',  50.00, '2024-03-24 16:00:00');

CREATE TABLE ms_returns (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES ms_customers(id),
    refund_amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

-- Returns totals: Jan=130, Feb=180, Mar=250, Grand=560
INSERT INTO ms_returns (id, customer_id, refund_amount, created_at) VALUES
    -- January 2024 — customer 1 (Alice)
    (1, 1, 50.00,  '2024-01-08 10:00:00'),
    (2, 1, 80.00,  '2024-01-22 14:00:00'),
    -- February 2024 — customer 2 (Bob)
    (3, 2, 100.00, '2024-02-10 11:00:00'),
    (4, 2, 80.00,  '2024-02-20 16:00:00'),
    -- March 2024 — customer 3 (Charlie)
    (5, 3, 150.00, '2024-03-05 09:00:00'),
    (6, 3, 100.00, '2024-03-15 13:00:00');
