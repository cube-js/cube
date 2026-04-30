DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    status TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    customer_id INTEGER REFERENCES customers(id),
    quantity INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

INSERT INTO customers (id, name, city) VALUES
    (1, 'Alice', 'New York'),
    (2, 'Bob', 'Boston'),
    (3, 'Charlie', 'San Francisco');

INSERT INTO products (id, name, category) VALUES
    (1, 'Laptop', 'Electronics'),
    (2, 'Phone', 'Electronics'),
    (3, 'Book', 'Education'),
    (4, 'Tablet', 'Electronics');

INSERT INTO orders (id, customer_id, status, amount, created_at) VALUES
    (1, 1, 'completed', 100.00, '2024-01-15 10:00:00'),
    (2, 1, 'completed', 200.00, '2024-02-20 14:00:00'),
    (3, 2, 'pending',   150.00, '2024-01-25 09:00:00'),
    (4, 3, 'completed',  80.00, '2024-03-10 11:00:00'),
    (5, NULL, 'cancelled', 50.00, '2024-03-15 16:00:00'),
    (6, 2, 'completed', 300.00, '2024-04-01 08:00:00');

INSERT INTO order_items (id, order_id, product_id, customer_id, quantity, price) VALUES
    (1,  1, 1, 1,    1, 100.00),
    (2,  1, 2, 1,    2,  50.00),
    (3,  2, 3, 1,    1,  30.00),
    (4,  2, 1, 1,    3, 100.00),
    (5,  3, 1, 2,    1, 100.00),
    (6,  3, 4, 2,    1,  80.00),
    (7,  4, 2, 3,    3,  50.00),
    (8,  4, 3, 3,    1,  30.00),
    (9,  5, 3, NULL,  2,  30.00),
    (10, 6, 1, 2,    1, 100.00);
