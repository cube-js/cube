DROP TABLE IF EXISTS dvf_orders CASCADE;

-- `currency` lives only in the model as a virtual `type: switch` dimension —
-- there is no `currency` column here on purpose. `country` drives the
-- switch-case measure.
CREATE TABLE dvf_orders (
    id INTEGER PRIMARY KEY,
    country TEXT NOT NULL,
    amount_usd NUMERIC(10, 2) NOT NULL,
    amount_eur NUMERIC(10, 2) NOT NULL
);

-- Five base rows × 3 switch values = 15 cells in the union; the default
-- filter pins it to the USD branch, leaving 5 cells (one per base row).
-- Without the default filter the user would see 15.
INSERT INTO dvf_orders (id, country, amount_usd, amount_eur) VALUES
    (1, 'US', 100.00, 92.00),
    (2, 'CA', 50.00, 46.00),
    (3, 'DE', 80.00, 75.00),
    (4, 'FR', 30.00, 28.00),
    (5, 'GB', 60.00, 56.00);
