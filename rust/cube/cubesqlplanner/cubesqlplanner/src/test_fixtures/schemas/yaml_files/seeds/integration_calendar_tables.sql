DROP TABLE IF EXISTS cal_orders CASCADE;

CREATE TABLE cal_orders (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO cal_orders (id, created_at)
SELECT
    gs.id,
    make_timestamp(
        2024 + (CASE WHEN gs.id < 41 THEN 0 ELSE 1 END),
        (gs.id % 12) + 1,
        1 + (gs.id * 7 % 25),
        0, 0, 0
    )
FROM generate_series(1, 80) AS gs(id);
