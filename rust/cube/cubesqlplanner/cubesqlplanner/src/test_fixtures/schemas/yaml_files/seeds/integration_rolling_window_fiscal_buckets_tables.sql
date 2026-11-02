DROP TABLE IF EXISTS fiscal_sales CASCADE;

-- One row per month; the fiscal year starts on May 1, so a calendar-year bucket mixes two
-- fiscal years. Months of FY2024 are worth 1, months of FY2025 are worth 10.
CREATE TABLE fiscal_sales AS
SELECT d::timestamp AS day,
       CASE WHEN d < '2025-05-01' THEN 1 ELSE 10 END AS amount
FROM generate_series('2024-05-01'::date, '2026-04-01'::date, interval '1 month') d;
