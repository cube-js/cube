DROP TABLE IF EXISTS checkouts CASCADE;
DROP TABLE IF EXISTS carts CASCADE;
DROP TABLE IF EXISTS sites CASCADE;

CREATE TABLE sites (
    id INTEGER PRIMARY KEY,
    country TEXT NOT NULL
);

CREATE TABLE carts (
    id INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES sites(id),
    msid TEXT NOT NULL,
    value NUMERIC(10, 2) NOT NULL
);

CREATE TABLE checkouts (
    id INTEGER PRIMARY KEY,
    cart_id INTEGER NOT NULL REFERENCES carts(id),
    msid TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL
);

INSERT INTO sites (id, country) VALUES
    (1, 'US'),
    (2, 'US'),
    (3, 'DE');

-- Two carts of site 1 share a msid, so a distinct count over them differs
-- from a plain count.
INSERT INTO carts (id, site_id, msid, value) VALUES
    (1, 1, 'm1', 10),
    (2, 1, 'm1', 20),
    (3, 2, 'm2', 30),
    (4, 3, 'm3', 40);

-- Cart 1 has two checkouts, so joining checkouts in splits its row. Carts 2
-- and 4 have none, so they only survive the join as NULL-extended rows.
INSERT INTO checkouts (id, cart_id, msid, amount) VALUES
    (1, 1, 'x1', 100),
    (2, 1, 'x2', 200),
    (3, 3, 'x1', 300);
