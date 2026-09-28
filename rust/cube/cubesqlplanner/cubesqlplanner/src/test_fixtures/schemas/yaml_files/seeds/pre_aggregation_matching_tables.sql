DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    city TEXT NOT NULL,
    priority TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

INSERT INTO orders (id, status, city, priority, amount, created_at, updated_at) VALUES
    -- Day 1: two high-priority in same city, different status
    (1,  'completed', 'New York', 'high',   100.00, '2025-01-10 08:00:00', '2025-01-10 09:00:00'),
    (2,  'pending',   'New York', 'high',   200.00, '2025-01-10 23:59:59', '2025-01-11 00:00:00'),
    -- Day 2: same city different priority
    (3,  'completed', 'New York', 'low',     50.00, '2025-01-11 00:00:00', '2025-01-11 01:00:00'),
    -- Month boundary
    (4,  'completed', 'Boston',   'high',   300.00, '2025-01-31 23:59:59', '2025-02-01 00:00:00'),
    (5,  'pending',   'Boston',   'medium', 150.00, '2025-02-01 00:00:00', '2025-02-01 01:00:00'),
    -- Same day, same status, same city — tests aggregation
    (6,  'completed', 'Boston',   'low',     75.00, '2025-02-01 10:00:00', '2025-02-01 11:00:00'),
    -- Cancelled — unique status for count_distinct
    (7,  'cancelled', 'Chicago',  'high',    25.00, '2025-02-15 12:00:00', '2025-02-15 13:00:00'),
    -- Wide date gap
    (8,  'completed', 'Chicago',  'low',    400.00, '2025-03-01 00:00:01', '2025-03-01 00:01:00'),
    -- Duplicate status+city combo across days
    (9,  'pending',   'Chicago',  'high',   175.00, '2025-03-01 08:00:00', '2025-03-01 09:00:00'),
    (10, 'pending',   'New York', 'medium',  60.00, '2025-03-02 14:00:00', '2025-03-02 15:00:00');
