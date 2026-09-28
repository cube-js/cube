DROP TABLE IF EXISTS rwg_orders CASCADE;

CREATE TABLE rwg_orders (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP NOT NULL
);

-- 1 row in 2024-01, then a 3-month gap, then 5 rows in 2024-05.
-- Designed to mirror the JS "rolling count without date range"
-- scenario: empty months between data points should still surface
-- as rows in the rolling time-series, with rolling count trailing
-- off to null in the middle.
INSERT INTO rwg_orders (id, created_at) VALUES
    (1, '2024-01-15 10:00:00'),
    (2, '2024-05-02 09:00:00'),
    (3, '2024-05-08 11:00:00'),
    (4, '2024-05-15 13:00:00'),
    (5, '2024-05-22 14:00:00'),
    (6, '2024-05-29 16:00:00');
