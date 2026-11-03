DROP TABLE IF EXISTS sqo_orders CASCADE;
DROP TABLE IF EXISTS sqo_customers CASCADE;

CREATE TABLE sqo_customers (
    id INTEGER PRIMARY KEY
);

INSERT INTO sqo_customers (id) VALUES (1), (2);

CREATE TABLE sqo_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES sqo_customers(id)
);

-- Three orders have no customer.
INSERT INTO sqo_orders (id, customer_id) VALUES
    (1, 1),
    (2, 1),
    (3, 2),
    (4, NULL),
    (5, NULL),
    (6, NULL);
