DROP TABLE IF EXISTS ms_pa_ts_orders CASCADE;

CREATE TABLE ms_pa_ts_orders (
    id INTEGER PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO ms_pa_ts_orders (id, status, amount, created_at) VALUES
    -- December 2024
    (1, 'new',        80.00, '2024-12-10'),
    (2, 'active',    120.00, '2024-12-20'),
    -- January 2025
    (3, 'new',       100.00, '2025-01-15'),
    (4, 'active',    150.00, '2025-01-20'),
    -- February 2025
    (5, 'new',       200.00, '2025-02-10'),
    (6, 'active',    250.00, '2025-02-15'),
    (7, 'completed', 300.00, '2025-02-20'),
    -- March 2025
    (8, 'completed', 400.00, '2025-03-10'),
    (9, 'active',    350.00, '2025-03-25');
