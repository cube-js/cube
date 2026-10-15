DROP TABLE IF EXISTS mf_dates CASCADE;
DROP TABLE IF EXISTS mf_products CASCADE;
DROP TABLE IF EXISTS mf_demand_lines CASCADE;
DROP TABLE IF EXISTS mf_visit_rows CASCADE;

-- A day's comparable prior-year day is 364 days back, except for one band in
-- which it is 371, so no plain interval reproduces the mapping.
CREATE TABLE mf_dates AS
SELECT d::timestamp AS date_val,
       (d::date - CASE WHEN d::date BETWEEN DATE '2026-06-21' AND DATE '2026-06-27'
                       THEN 371 ELSE 364 END)::timestamp AS mapped_prior_date,
       (d::date - extract(dow FROM d)::int)::timestamp AS week_begin
FROM generate_series(DATE '2025-01-01', DATE '2026-12-31', INTERVAL '1 day') g(d);

-- Two categories, each split over several subcategories and departments.
CREATE TABLE mf_products (product_id, category, subcategory, department) AS
VALUES ('p1', 'home', 'kitchen', 'cookware'),
       ('p2', 'home', 'kitchen', 'cutlery'),
       ('p3', 'home', 'bath', 'towels'),
       ('p4', 'apparel', 'women', 'dresses'),
       ('p5', 'apparel', 'men', 'shirts'),
       ('p6', 'apparel', 'men', 'shoes');

-- One line per (day, product) with an amount encoding both, plus extra
-- lines sharing an order reference, so a distinct count differs from a count.
CREATE TABLE mf_demand_lines AS
SELECT row_number() OVER (ORDER BY c.date_val, p.product_id, n) AS id,
       c.date_val                                                AS partition_date,
       p.product_id,
       (c.date_val::date - DATE '2025-01-01') + p.ord * 1000 + n AS amount,
       p.product_id || '-' || to_char(c.date_val, 'YYYYMMDD')    AS order_ref
FROM mf_dates c
         CROSS JOIN (SELECT product_id, row_number() OVER (ORDER BY product_id) AS ord
                     FROM mf_products) p
         CROSS JOIN generate_series(1, 2) n
WHERE c.date_val < TIMESTAMP '2026-07-01';

CREATE TABLE mf_visit_rows AS
SELECT row_number() OVER (ORDER BY c.date_val, p.product_id) AS id,
       c.date_val                                            AS partition_date,
       p.product_id,
       10                                                    AS visits
FROM mf_dates c
         CROSS JOIN mf_products p
WHERE c.date_val < TIMESTAMP '2026-07-01';
