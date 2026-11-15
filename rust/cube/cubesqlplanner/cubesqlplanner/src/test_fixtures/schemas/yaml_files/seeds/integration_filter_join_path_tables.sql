DROP TABLE IF EXISTS sp_sales CASCADE;
DROP TABLE IF EXISTS gross_sales CASCADE;
DROP TABLE IF EXISTS hcps CASCADE;
DROP TABLE IF EXISTS brands CASCADE;

CREATE TABLE brands (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE hcps (
    id INTEGER PRIMARY KEY
);

CREATE TABLE gross_sales (
    id INTEGER PRIMARY KEY,
    hcp_id INTEGER NOT NULL REFERENCES hcps(id),
    brand_id INTEGER NOT NULL REFERENCES brands(id)
);

CREATE TABLE sp_sales (
    id INTEGER PRIMARY KEY,
    hcp_id INTEGER NOT NULL REFERENCES hcps(id),
    brand_id INTEGER NOT NULL REFERENCES brands(id),
    amount NUMERIC(10, 2) NOT NULL
);

INSERT INTO brands VALUES (1, 'A'), (2, 'B');
INSERT INTO hcps VALUES (1), (2);
INSERT INTO gross_sales VALUES (1, 1, 2), (2, 2, 1), (3, 2, 2);
INSERT INTO sp_sales VALUES
    (1, 1, 1, 10.00),
    (2, 1, 2, 20.00),
    (3, 2, 1, 40.00);
