DROP TABLE IF EXISTS test_data CASCADE;

CREATE TABLE test_data (
    created_at TIMESTAMP NOT NULL,
    val NUMERIC(10, 2) NOT NULL
);

INSERT INTO test_data (created_at, val) VALUES
    ('2025-10-05 10:00:00', 10.00),
    ('2025-10-06 11:00:00', 20.00),
    ('2025-10-07 09:00:00', 30.00),
    ('2025-10-07 15:00:00', 40.00),
    ('2025-10-08 08:00:00', 50.00),
    ('2025-10-09 12:00:00', 60.00);
