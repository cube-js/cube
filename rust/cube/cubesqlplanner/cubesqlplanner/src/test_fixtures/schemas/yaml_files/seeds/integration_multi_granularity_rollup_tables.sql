DROP TABLE IF EXISTS mg_activity CASCADE;

CREATE TABLE mg_activity (
    id          INTEGER PRIMARY KEY,
    app_name    TEXT      NOT NULL,
    cohort_date TIMESTAMP NOT NULL,
    amount      NUMERIC   NOT NULL
);

-- Two rows per day over 2024-11-25 .. 2025-02-07, so days, weeks, months,
-- quarters and years all hold different totals. Amounts encode the day offset.
INSERT INTO mg_activity (id, app_name, cohort_date, amount)
SELECT row_number() OVER (ORDER BY d.day, app.name),
       app.name,
       d.day + INTERVAL '13 hours',
       (d.off + 1) * app.factor
FROM (SELECT (DATE '2024-11-25' + (gs.n - 1))::timestamp AS day,
             gs.n - 1                                    AS off
      FROM generate_series(1, 75) AS gs(n)) d
         CROSS JOIN (VALUES ('alpha', 1), ('beta', 100)) AS app(name, factor);
