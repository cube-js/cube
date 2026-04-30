DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    currency TEXT NOT NULL,
    amount_usd NUMERIC(10, 2) NOT NULL DEFAULT 0,
    amount_eur NUMERIC(10, 2) NOT NULL DEFAULT 0,
    amount_gbp NUMERIC(10, 2) NOT NULL DEFAULT 0
);

INSERT INTO orders (id, date, currency, amount_usd, amount_eur, amount_gbp) VALUES
    (1, '2024-03-15 10:00:00', 'USD', 100.00, 0, 0),
    (2, '2024-06-20 11:00:00', 'USD', 200.00, 0, 0),
    (3, '2024-09-10 09:00:00', 'EUR', 0, 150.00, 0),
    (4, '2025-01-05 14:00:00', 'EUR', 0, 250.00, 0),
    (5, '2025-04-12 08:00:00', 'GBP', 0, 0, 300.00),
    (6, '2025-07-22 16:00:00', 'USD', 350.00, 0, 0);
