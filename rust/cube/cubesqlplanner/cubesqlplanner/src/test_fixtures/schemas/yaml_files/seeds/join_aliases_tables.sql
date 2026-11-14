DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS invoices CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS employees CASCADE;

CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO departments (id, name) VALUES
    (1, 'Sales'),
    (2, 'Ops');

CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    city TEXT NOT NULL,
    department_id INTEGER NOT NULL,
    score INTEGER NOT NULL
);

INSERT INTO users (id, city, department_id, score) VALUES
    (1, 'Berlin', 1, 10),
    (2, 'Paris', 2, 20),
    (3, 'Berlin', 2, 30);

CREATE TABLE reviews (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL
);

INSERT INTO reviews (id, user_id) VALUES
    (1, 1),
    (2, 1),
    (3, 1),
    (4, 2);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO products (id, name) VALUES
    (1, 'A'),
    (2, 'B');

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    manager_id INTEGER NOT NULL,
    product_id INTEGER,
    status TEXT NOT NULL,
    amount INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);

INSERT INTO orders (id, customer_id, manager_id, product_id, status, amount, created_at, completed_at) VALUES
    (1, 1, 2, 1, 'completed', 100, '2025-01-05', '2025-02-10'),
    (2, 1, 2, 2, 'pending', 50, '2025-01-20', '2025-03-01'),
    (3, 2, 3, 1, 'completed', 70, '2024-01-07', '2024-01-09'),
    (4, 3, 2, NULL, 'pending', 30, '2025-02-02', NULL);

CREATE TABLE invoices (
    id INTEGER PRIMARY KEY,
    payer_id INTEGER NOT NULL
);

INSERT INTO invoices (id, payer_id) VALUES
    (1, 1),
    (2, 3);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    supervisor_id INTEGER
);

INSERT INTO employees (id, name, supervisor_id) VALUES
    (1, 'Ann', NULL),
    (2, 'Bob', 1),
    (3, 'Cid', 1),
    (4, 'Dan', 2);
