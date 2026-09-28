DROP TABLE IF EXISTS cs_switch_sales CASCADE;
DROP TABLE IF EXISTS cs_switch_accounts CASCADE;

CREATE TABLE cs_switch_accounts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE cs_switch_sales (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES cs_switch_accounts(id),
    category TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    amount NUMERIC NOT NULL
);

INSERT INTO cs_switch_accounts (id, name) VALUES
    (1, 'acme'),
    (2, 'globex');

-- One row per (account, category, month) with a distinct amount per month, so trailing
-- windows of different lengths produce distinct sums. The 2023 rows put data
-- outside the queried year, which is what separates a 12-month trailing window
-- from year-to-date: without them the two coincide on every query below.
INSERT INTO cs_switch_sales (id, account_id, category, created_at, amount) VALUES
    -- Early 2023, so a year-over-year time shift has something to subtract and
    -- the derived entrypoints are not all NULL on their to-date branch. Only the
    -- May row falls inside a 12-month trailing window ending in 2024.
    (19, 1, 'books', '2023-02-15 00:00:00', 20),
    (20, 1, 'books', '2023-05-15 00:00:00', 30),
    (21, 2, 'toys',  '2023-02-15 00:00:00', 2),
    (22, 2, 'toys',  '2023-05-15 00:00:00', 3),
    (13, 1, 'books', '2023-11-15 00:00:00', 7),
    (14, 1, 'books', '2023-12-15 00:00:00', 50),
    (15, 2, 'toys',  '2023-11-15 00:00:00', 3),
    (16, 2, 'toys',  '2023-12-15 00:00:00', 5),
    (1,  1, 'books', '2024-01-15 00:00:00', 100),
    (2,  1, 'books', '2024-02-15 00:00:00', 200),
    (3,  1, 'books', '2024-03-15 00:00:00', 300),
    (4,  1, 'books', '2024-04-15 00:00:00', 400),
    (5,  1, 'books', '2024-05-15 00:00:00', 500),
    (6,  1, 'books', '2024-06-15 00:00:00', 600),
    (7,  2, 'toys',  '2024-01-15 00:00:00', 10),
    (8,  2, 'toys',  '2024-02-15 00:00:00', 20),
    (9,  2, 'toys',  '2024-03-15 00:00:00', 30),
    (10, 2, 'toys',  '2024-04-15 00:00:00', 40),
    (11, 2, 'toys',  '2024-05-15 00:00:00', 50),
    (12, 2, 'toys',  '2024-06-15 00:00:00', 60),
    -- Each account sells both categories, so grouping by the joined cube's
    -- dimension is distinguishable from grouping by the local one.
    (17, 1, 'toys',  '2024-05-15 00:00:00', 1),
    (18, 2, 'books', '2024-05-15 00:00:00', 2);
