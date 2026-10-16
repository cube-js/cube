DROP TABLE IF EXISTS cs_part_sales CASCADE;

CREATE TABLE cs_part_sales (
    id SERIAL PRIMARY KEY,
    hcp TEXT NOT NULL,
    brand TEXT NOT NULL,
    sold_at TIMESTAMP NOT NULL,
    amount NUMERIC NOT NULL
);

-- Four months, so the monthly rollup is split into four partition tables. Each
-- (hcp, brand, month) cell has three rows on different days, so the rollup does
-- aggregate, and every cell sum is distinct, so ordering by the measure is total.
INSERT INTO cs_part_sales (hcp, brand, sold_at, amount)
SELECT 'h' || h, 'b' || b, make_timestamp(2024, m, d, 0, 0, 0), h * 1000 + b * 100 + m * 10 + d
FROM generate_series(1, 4) h,
     generate_series(1, 3) b,
     generate_series(1, 4) m,
     generate_series(1, 3) d
WHERE (h + b + m) % 4 <> 0;
