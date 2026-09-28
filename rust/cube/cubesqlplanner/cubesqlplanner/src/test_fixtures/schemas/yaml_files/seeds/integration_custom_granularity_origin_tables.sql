DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

-- Rows sit on the exact edges of the grids the granularities below define, so a grouping that
-- lost the origin and fell back to the natural boundary shows up as a moved row.
INSERT INTO orders (id, amount, created_at) VALUES
    (1,  5.00,  '2023-04-01 00:00:00'), -- first instant of the fiscal year 2023
    (2,  15.00, '2024-03-31 23:59:59'), -- last instant of the fiscal year 2023
    (3,  10.00, '2024-04-01 00:00:00'), -- first instant of the fiscal year 2024
    (4,  20.00, '2024-12-31 12:00:00'), -- calendar year end, still the fiscal year 2024
    (5,  30.00, '2025-01-14 23:59:59'), -- last instant of the month starting on 2024-12-15
    (6,  40.00, '2025-01-15 00:00:00'), -- first instant of the month starting on 2025-01-15
    (7,  50.00, '2025-03-31 23:59:59'), -- last instant of the fiscal year 2024
    (8,  60.00, '2025-04-01 00:00:00'), -- first instant of the fiscal year 2025
    (9,  70.00, '2025-09-10 08:00:00'),
    (10, 80.00, '2026-03-31 23:59:59'); -- last instant of the fiscal year 2025
