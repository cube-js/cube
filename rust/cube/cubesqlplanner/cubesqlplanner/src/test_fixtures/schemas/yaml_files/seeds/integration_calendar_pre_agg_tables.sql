DROP TABLE IF EXISTS cal_pa_dates CASCADE;
DROP TABLE IF EXISTS cal_pa_demand CASCADE;

-- Retail calendar: 728 days starting Sun 2024-02-04, two 364-day retail years.
-- `prev_year_date` maps a day to the same retail weekday one retail year back,
-- which is 364 days, not a calendar year.
CREATE TABLE cal_pa_dates (
    date_val          TIMESTAMP PRIMARY KEY,
    retail_year       TEXT      NOT NULL,
    retail_week_begin TIMESTAMP NOT NULL,
    retail_year_week  TEXT      NOT NULL,
    prev_year_date    TIMESTAMP
);

INSERT INTO cal_pa_dates
SELECT d.date_val,
       CASE WHEN d.off < 364 THEN '2024' ELSE '2025' END,
       d.date_val - ((d.off % 7) * INTERVAL '1 day'),
       (CASE WHEN d.off < 364 THEN '2024' ELSE '2025' END)
           || '-WK' || LPAD(((((d.off % 364) / 7) + 1))::text, 2, '0'),
       CASE WHEN d.off >= 364 THEN d.date_val - INTERVAL '364 day' END
FROM (SELECT (DATE '2024-02-04' + (gs.n - 1))::timestamp AS date_val,
             gs.n - 1                                    AS off
      FROM generate_series(1, 728) AS gs(n)) d;

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
      FROM generate_series(1, 728) AS gs(n)) d
         CROSS JOIN (VALUES ('apparel', 1), ('grocery', 10)) AS dept(name, factor);
