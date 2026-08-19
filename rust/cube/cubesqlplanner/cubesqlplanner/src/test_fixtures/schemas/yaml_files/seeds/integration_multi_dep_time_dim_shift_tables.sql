DROP TABLE IF EXISTS mdd_events CASCADE;

CREATE TABLE mdd_events (
    id INTEGER PRIMARY KEY,
    happened_at TIMESTAMP,
    recorded_at TIMESTAMP,
    val NUMERIC(10, 2) NOT NULL
);

-- Row 3 has no happened_at, so the shift does not move it: it stays in
-- February while every other row shifts by a month. That is what makes the
-- shifted values impossible to reproduce by offsetting the stored column.
INSERT INTO mdd_events (id, happened_at, recorded_at, val) VALUES
    (1, '2023-12-12 10:00:00', NULL,                  1000.00),
    (2, '2024-01-15 10:00:00', NULL,                  2000.00),
    (3, NULL,                  '2024-02-14 10:00:00',  500.00),
    (4, '2024-03-05 10:00:00', NULL,                  1000.00);
