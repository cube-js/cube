DROP TABLE IF EXISTS pa_returns CASCADE;
DROP TABLE IF EXISTS pa_customers CASCADE;

CREATE TABLE pa_customers (
    id INTEGER PRIMARY KEY,
    lifetime_value NUMERIC(10, 2) NOT NULL
);

INSERT INTO pa_customers (id, lifetime_value) VALUES
    (1, 1000.00),
    (2, 2000.00),
    (3,  500.00);

CREATE TABLE pa_returns (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES pa_customers(id),
    created_at TIMESTAMP NOT NULL
);

-- One customer per month, so each month's total_value is that customer's
-- lifetime_value. December 2023 sits before the queried range and is only
-- reachable when the shifted leaf widens its pre-aggregation date range.
INSERT INTO pa_returns (id, customer_id, created_at) VALUES
    (1, 1, '2023-12-12 10:00:00'),
    (2, 2, '2024-01-15 10:00:00'),
    (3, 3, '2024-02-14 10:00:00'),
    (4, 1, '2024-03-05 10:00:00');
