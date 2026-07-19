DROP TABLE IF EXISTS ms_pa_orders CASCADE;

CREATE TABLE ms_pa_orders (
    id INTEGER PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);

INSERT INTO ms_pa_orders (id, status, amount) VALUES
    (1, 'new',       100.00),
    (2, 'new',       200.00),
    (3, 'active',    150.00),
    (4, 'active',    250.00),
    (5, 'active',    350.00),
    (6, 'completed', 300.00),
    (7, 'completed', 400.00);
