-- One row per day carrying amount 1, so a rolling sum reads as the number of
-- days its window covered.

DROP TABLE IF EXISTS fprw_sales CASCADE;

CREATE TABLE fprw_sales AS
SELECT gs.n                                        AS id,
       (DATE '2024-01-01' + (gs.n - 1))::timestamp AS day_d,
       1                                           AS amount
FROM generate_series(1, 200) AS gs(n);
