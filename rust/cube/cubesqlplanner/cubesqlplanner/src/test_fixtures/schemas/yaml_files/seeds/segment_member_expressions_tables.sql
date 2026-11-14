-- Orders with a customer, without one, and with a dangling customer id; two
-- orders share a customer so the many_to_one side repeats.

DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id   int,
    name text
);

INSERT INTO customers (id, name) VALUES
    (1, 'Alice'),
    (2, 'Bob');

CREATE TABLE orders (
    id          int,
    status      text,
    customer_id int
);

INSERT INTO orders (id, status, customer_id) VALUES
    (1, 'new',  1),
    (2, 'new',  NULL),
    (3, 'done', 2),
    (4, 'done', 99),
    (5, 'done', 2);
