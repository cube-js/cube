DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO orders (id, status, amount, created_at) VALUES
    (1,  'processing', 10.00,  '2024-01-15 10:00:00'),
    (2,  'completed',  20.00,  '2024-02-10 11:00:00'),
    (3,  'shipped',    30.00,  '2024-03-20 09:00:00'),
    (4,  'processing', 40.00,  '2024-05-01 14:00:00'),
    (5,  'completed',  50.00,  '2024-06-15 08:00:00'),
    (6,  'shipped',    60.00,  '2024-08-10 10:00:00'),
    (7,  'processing', 70.00,  '2024-09-20 11:00:00'),
    (8,  'completed',  80.00,  '2024-10-05 09:00:00'),
    (9,  'shipped',    90.00,  '2024-11-15 14:00:00'),
    (10, 'processing', 100.00, '2024-12-01 08:00:00'),
    (11, 'completed',  110.00, '2025-01-20 10:00:00'),
    (12, 'shipped',    120.00, '2025-04-10 11:00:00'),
    (13, 'processing', 130.00, '2025-07-15 09:00:00'),
    (14, 'completed',  140.00, '2025-09-01 14:00:00'),
    (15, 'shipped',    150.00, '2025-11-20 08:00:00');
