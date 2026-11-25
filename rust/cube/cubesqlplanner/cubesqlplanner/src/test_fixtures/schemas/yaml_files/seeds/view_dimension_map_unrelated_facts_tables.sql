DROP TABLE IF EXISTS vdo_income CASCADE;
DROP TABLE IF EXISTS vdo_expenses CASCADE;

CREATE TABLE vdo_income (
    id INTEGER PRIMARY KEY,
    branch TEXT NOT NULL,
    received_at TIMESTAMP NOT NULL,
    amount INTEGER NOT NULL
);

-- 2026-01: North 150, South 70. 2026-02: North 20, South 30.
INSERT INTO vdo_income (id, branch, received_at, amount) VALUES
    (1, 'North', '2026-01-05', 100),
    (2, 'North', '2026-01-20', 50),
    (3, 'South', '2026-01-10', 70),
    (4, 'South', '2026-02-03', 30),
    (5, 'North', '2026-02-14', 20);

CREATE TABLE vdo_expenses (
    id INTEGER PRIMARY KEY,
    branch TEXT NOT NULL,
    spent_at TIMESTAMP NOT NULL,
    amount INTEGER NOT NULL
);

-- 2026-01: North 40, South 25. 2026-02: South 60, East 10 (East has no income).
INSERT INTO vdo_expenses (id, branch, spent_at, amount) VALUES
    (1, 'North', '2026-01-07', 40),
    (2, 'South', '2026-01-15', 25),
    (3, 'South', '2026-02-20', 60),
    (4, 'East', '2026-02-01', 10);
