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

-- Orders with timestamps near midnight UTC to make timezone shifts visible.
-- America/New_York = UTC-5: 03:00 UTC → 22:00 previous day ET
-- Europe/Berlin = UTC+1: 23:30 UTC → 00:30 next day CET
INSERT INTO rw_orders (id, customer_id, status, category, amount, created_at) VALUES
    -- Jan 10 03:00 UTC → Jan 9 22:00 ET (shifts to previous day in NY)
    (1,  1, 'completed',  'electronics', 100.00, '2024-01-10 03:00:00'),
    -- Jan 10 15:00 UTC → stays Jan 10 in all timezones
    (2,  2, 'pending',    'books',        50.00, '2024-01-10 15:00:00'),
    -- Jan 12 02:00 UTC → Jan 11 21:00 ET (shifts to previous day in NY)
    (3,  3, 'completed',  'clothing',    200.00, '2024-01-12 02:00:00'),
    -- Jan 12 14:00 UTC → stays Jan 12
    (4,  1, 'cancelled',  'electronics',  75.00, '2024-01-12 14:00:00'),
    -- Jan 14 23:30 UTC → Jan 15 00:30 CET (shifts to next day in Berlin)
    (5,  2, 'completed',  'books',       150.00, '2024-01-14 23:30:00'),
    -- Jan 15 04:00 UTC → Jan 14 23:00 ET (shifts to previous day in NY)
    (6,  3, 'completed',  'electronics', 300.00, '2024-01-15 04:00:00'),
    -- Jan 16 12:00 UTC → stays Jan 16
    (7,  1, 'pending',    'clothing',     80.00, '2024-01-16 12:00:00'),
    -- Jan 18 01:00 UTC → Jan 17 20:00 ET, Jan 18 02:00 CET
    (8,  2, 'completed',  'books',       120.00, '2024-01-18 01:00:00'),
    -- Jan 18 23:00 UTC → Jan 19 00:00 CET (shifts to next day in Berlin)
    (9,  3, 'cancelled',  'electronics', 250.00, '2024-01-18 23:00:00'),
    -- Jan 20 03:30 UTC → Jan 19 22:30 ET (shifts to previous day in NY)
    (10, 1, 'completed',  'clothing',    180.00, '2024-01-20 03:30:00');
