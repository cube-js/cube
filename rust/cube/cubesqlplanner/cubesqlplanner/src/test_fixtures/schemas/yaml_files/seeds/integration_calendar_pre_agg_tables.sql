DROP TABLE IF EXISTS cal_pa_dates CASCADE;
DROP TABLE IF EXISTS cal_pa_demand CASCADE;

-- A 4-5-4 retail calendar starting Sun 2024-02-04. The first retail year is 53
-- weeks and the next two are 52, so "one year back" is 371 days for one band of
-- rows and 364 for another: no interval reproduces the mapping, which is the
-- point of storing it as a column.
CREATE TABLE cal_pa_dates (
    date_val          TIMESTAMP PRIMARY KEY,
    retail_year       TEXT      NOT NULL,
    retail_week_begin TIMESTAMP NOT NULL,
    retail_year_week  TEXT      NOT NULL,
    prev_year_date    TIMESTAMP
);

INSERT INTO cal_pa_dates
SELECT d.date_val,
       d.retail_year,
       d.date_val - ((d.off % 7) * INTERVAL '1 day'),
       d.retail_year || '-WK' || LPAD((((d.off - d.year_start) / 7) + 1)::text, 2, '0'),
       CASE
           WHEN d.off BETWEEN 371 AND 734 THEN d.date_val - INTERVAL '371 day'
           WHEN d.off >= 735 THEN d.date_val - INTERVAL '364 day'
       END
FROM (SELECT date_val,
             off,
             CASE WHEN off < 371 THEN '2024' WHEN off < 735 THEN '2025' ELSE '2026' END AS retail_year,
             CASE WHEN off < 371 THEN 0 WHEN off < 735 THEN 371 ELSE 735 END           AS year_start
      FROM (SELECT (DATE '2024-02-04' + (gs.n - 1))::timestamp AS date_val,
                   gs.n - 1                                    AS off
            FROM generate_series(1, 1099) AS gs(n)) s) d;

CREATE TABLE cal_pa_demand (
    id            INTEGER PRIMARY KEY,
    demand_date   TIMESTAMP NOT NULL,
    item_dept     TEXT      NOT NULL,
    gross_demand  NUMERIC   NOT NULL,
    cancellations NUMERIC   NOT NULL
);

-- One row per (day, department). Amounts encode the day offset so that any
-- window shift shows up as a different sum.
INSERT INTO cal_pa_demand (id, demand_date, item_dept, gross_demand, cancellations)
SELECT row_number() OVER (ORDER BY d.date_val, dept.name),
       d.date_val,
       dept.name,
       (d.off + 1) * dept.factor,
       1
FROM (SELECT (DATE '2024-02-04' + (gs.n - 1))::timestamp AS date_val,
             gs.n - 1                                    AS off
      FROM generate_series(1, 1099) AS gs(n)) d
         CROSS JOIN (VALUES ('apparel', 1), ('grocery', 10)) AS dept(name, factor);
