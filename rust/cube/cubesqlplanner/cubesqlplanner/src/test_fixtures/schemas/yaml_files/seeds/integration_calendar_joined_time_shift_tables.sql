DROP TABLE IF EXISTS jts_calendar CASCADE;
DROP TABLE IF EXISTS jts_sales CASCADE;

CREATE TABLE jts_calendar (
    date TIMESTAMP PRIMARY KEY,
    prior_peak TIMESTAMP NOT NULL
);

INSERT INTO jts_calendar (date, prior_peak)
SELECT d::timestamp, (d - interval '2 day')::timestamp
FROM generate_series('2024-01-01'::date, '2024-01-07'::date, interval '1 day') AS d;

CREATE TABLE jts_sales (
    id INTEGER PRIMARY KEY,
    sale_date TIMESTAMP NOT NULL,
    amount INTEGER NOT NULL
);

INSERT INTO jts_sales (id, sale_date, amount) VALUES
    (1, '2024-01-01', 5),
    (2, '2024-01-02', 10),
    (3, '2024-01-03', 20);
