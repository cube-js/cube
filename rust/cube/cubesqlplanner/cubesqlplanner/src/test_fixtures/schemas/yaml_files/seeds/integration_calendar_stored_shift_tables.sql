CREATE EXTENSION IF NOT EXISTS hll;
DROP TABLE IF EXISTS cal_ss_dates CASCADE;
DROP TABLE IF EXISTS cal_ss_sales CASCADE;

-- A 4-5-4 NRF retail calendar: each retail year ends on the Saturday nearest
-- Jan 31, which makes retail 2006 a 53-week year. A day's prior-year date is
-- the same weekday of the same week number a year back, and week 53 compares
-- to week 52: two reporting days then share one prior day, and "one year
-- back" is 371 days after the long year but 364 everywhere else.
CREATE TABLE cal_ss_dates AS
WITH ends AS (
    SELECT y,
           make_date(y + 1, 1, 31)
               + (CASE WHEN ((6 - extract(dow FROM make_date(y + 1, 1, 31))::int + 7) % 7) <= 3
                       THEN ((6 - extract(dow FROM make_date(y + 1, 1, 31))::int + 7) % 7)
                       ELSE ((6 - extract(dow FROM make_date(y + 1, 1, 31))::int + 7) % 7) - 7 END) AS year_end
    FROM generate_series(2003, 2008) y),
     years AS (
         SELECT y AS retail_year, lag(year_end) OVER (ORDER BY y) + 1 AS year_begin, year_end
         FROM ends),
     days AS (
         SELECT d::date                            AS date_val,
                r.retail_year,
                r.year_begin,
                ((d::date - r.year_begin) / 7) + 1 AS retail_week,
                ((d::date - r.year_begin) % 7)     AS weekday
         FROM years r, generate_series(r.year_begin, r.year_end, INTERVAL '1 day') d
         WHERE r.year_begin IS NOT NULL)
SELECT c.date_val::timestamp                 AS date_val,
       c.retail_year::text                   AS retail_year,
       c.retail_week,
       c.year_begin::timestamp               AS retail_year_begin,
       (c.date_val - c.weekday)::timestamp   AS retail_week_begin,
       p.date_val::timestamp                 AS prev_year_date
FROM days c
         LEFT JOIN days p
                   ON p.retail_year = c.retail_year - 1
                       AND p.retail_week = LEAST(c.retail_week, 52)
                       AND p.weekday = c.weekday;

-- One row per (day, store). Amounts encode the day so that any mapping error
-- shows up as a different sum.
CREATE TABLE cal_ss_sales AS
SELECT row_number() OVER (ORDER BY c.date_val, s.store) AS id,
       c.date_val                                       AS sale_date,
       s.store,
       (c.date_val::date - DATE '2004-01-01') * s.factor  AS amount
FROM cal_ss_dates c
         CROSS JOIN (VALUES ('north', 1), ('south', 10)) AS s(store, factor);
