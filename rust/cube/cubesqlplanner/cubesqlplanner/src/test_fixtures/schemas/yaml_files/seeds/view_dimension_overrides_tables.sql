DROP TABLE IF EXISTS vdo_calendar CASCADE;
DROP TABLE IF EXISTS vdo_subscriptions CASCADE;
DROP TABLE IF EXISTS vdo_carts CASCADE;

CREATE TABLE vdo_calendar AS
SELECT d::date AS dt_ts, to_char(d, 'YYYY') AS dt_year, to_char(d, 'YYYY-MM') AS dt_year_month
FROM generate_series(DATE '2025-01-01', DATE '2026-12-31', INTERVAL '1 day') d;

CREATE TABLE vdo_subscriptions (
    id INTEGER PRIMARY KEY,
    plan TEXT NOT NULL,
    purchase_date DATE NOT NULL,
    cancel_date DATE
);

-- 2026 purchases: 3 (2026-01), 4 (2026-02). 2026 cancellations: 1, 3 (both 2026-02).
INSERT INTO vdo_subscriptions (id, plan, purchase_date, cancel_date) VALUES
    (1, 'pro', '2025-03-10', '2026-02-05'),
    (2, 'basic', '2025-06-01', '2025-09-01'),
    (3, 'pro', '2026-01-15', '2026-02-20'),
    (4, 'basic', '2026-02-01', NULL);

CREATE TABLE vdo_carts (
    id INTEGER PRIMARY KEY,
    created_date DATE NOT NULL
);

INSERT INTO vdo_carts (id, created_date) VALUES
    (1, '2026-02-03'),
    (2, '2026-02-10'),
    (3, '2025-09-01');
