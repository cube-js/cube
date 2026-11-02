DROP TABLE IF EXISTS sqn_orders CASCADE;
DROP TABLE IF EXISTS sqn_customers CASCADE;
DROP TABLE IF EXISTS sqn_regions CASCADE;

CREATE TABLE sqn_regions (
    id INTEGER PRIMARY KEY
);

-- Region 3 has no customers.
INSERT INTO sqn_regions (id) VALUES (1), (2), (3);

CREATE TABLE sqn_customers (
    id INTEGER PRIMARY KEY,
    region_id INTEGER REFERENCES sqn_regions(id)
);

-- Customer 4 has no orders.
INSERT INTO sqn_customers (id, region_id) VALUES
    (1, 1),
    (2, 1),
    (3, 2),
    (4, 2);

CREATE TABLE sqn_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES sqn_customers(id),
    amount INTEGER
);

INSERT INTO sqn_orders (id, customer_id, amount) VALUES
    (1, 1, 10),
    (2, 1, 20),
    (3, 2, 5),
    (4, 3, 7);
