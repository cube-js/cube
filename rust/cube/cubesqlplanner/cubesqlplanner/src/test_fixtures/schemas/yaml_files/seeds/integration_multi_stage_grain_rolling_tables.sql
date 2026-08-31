CREATE TABLE grain_returns (
    day DATE NOT NULL,
    security VARCHAR(16) NOT NULL,
    irr NUMERIC NOT NULL,
    weight NUMERIC NOT NULL
);

-- Two equally weighted rows per day, so the weighted daily factor is the row
-- factor: 1.10, 1.20, 0.50. Linked over the three days that is exactly -0.34.
INSERT INTO grain_returns (day, security, irr, weight) VALUES
    ('2024-01-01', 'A',  10.0, 100.0),
    ('2024-01-01', 'B',  10.0, 100.0),
    ('2024-01-02', 'A',  20.0, 100.0),
    ('2024-01-02', 'B',  20.0, 100.0),
    ('2024-01-03', 'A', -50.0, 100.0),
    ('2024-01-03', 'B', -50.0, 100.0);
