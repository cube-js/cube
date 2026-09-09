-- A 364-day fiscal calendar: three years of 52 seven-day weeks starting
-- 2023-01-01, so no nominal year interval reproduces the mapping.
--
-- Day n of the calendar carries amount n, so a value read from the fiscal year
-- before reads n - 364 and two years before n - 728, while a nominal `1 year`
-- shift lands one day off at n - 365.

DROP TABLE IF EXISTS fpts_calendar CASCADE;

CREATE TABLE fpts_calendar AS
SELECT (DATE '2023-01-01' + (gs.n - 1))::timestamp AS d,
       CASE
           WHEN gs.n > 364 THEN (DATE '2023-01-01' + (gs.n - 1 - 364))::timestamp
           END                                     AS d_prev_fy,
       CASE
           WHEN gs.n > 728 THEN (DATE '2023-01-01' + (gs.n - 1 - 728))::timestamp
           END                                     AS d_prev_two_fy,
       'FY' || (((gs.n - 1) / 364) + 1)            AS fy_name
FROM generate_series(1, 1092) AS gs(n);

DROP TABLE IF EXISTS fpts_sales CASCADE;

CREATE TABLE fpts_sales AS
SELECT gs.n                                        AS id,
       (DATE '2023-01-01' + (gs.n - 1))::timestamp AS day_d,
       'FY' || (((gs.n - 1) / 364) + 1)            AS fy_name,
       gs.n                                        AS amount
FROM generate_series(1, 1092) AS gs(n);
