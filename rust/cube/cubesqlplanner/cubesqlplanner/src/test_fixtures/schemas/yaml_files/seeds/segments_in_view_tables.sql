DROP TABLE IF EXISTS tickets CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;

CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    region TEXT NOT NULL
);

CREATE TABLE tickets (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(id)
);

-- Account 1: 2 tickets (hasNoTickets = false)
-- Account 2: 0 tickets (hasNoTickets = true)
-- Account 3: 1 ticket  (hasNoTickets = false)
-- Account 4: 0 tickets (hasNoTickets = true)
INSERT INTO accounts (id, region) VALUES
    (1, 'US'),
    (2, 'US'),
    (3, 'EU'),
    (4, 'EU');

INSERT INTO tickets (id, account_id) VALUES
    (1, 1),
    (2, 1),
    (3, 3);
