DROP TABLE IF EXISTS pa_sales CASCADE;

CREATE TABLE pa_sales (
    id INTEGER PRIMARY KEY,
    brand VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);

-- Every day totals 100 across three brands, so acme's share is a clean
-- fraction and stays well away from 1.0 — a denominator that kept the brand
-- filter would read as a share of exactly 1.
INSERT INTO pa_sales (id, brand, created_at, amount) VALUES
    (1, 'acme',    '2024-01-01 10:00:00',  30.00),
    (2, 'globex',  '2024-01-01 11:00:00',  50.00),
    (3, 'initech', '2024-01-01 12:00:00',  20.00),
    (4, 'acme',    '2024-01-02 10:00:00',  40.00),
    (5, 'globex',  '2024-01-02 11:00:00',  40.00),
    (6, 'initech', '2024-01-02 12:00:00',  20.00);
