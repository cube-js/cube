DROP TABLE IF EXISTS case_switch_orders;
CREATE TABLE case_switch_orders (
    id INT,
    status TEXT,
    amount_usd NUMERIC,
    amount_eur NUMERIC,
    amount_nok NUMERIC
);
INSERT INTO case_switch_orders VALUES
    (1, 'a', 10, 9, 100),
    (2, 'b', 20, 18, 200),
    (3, 'b', 5, 4, 50);
