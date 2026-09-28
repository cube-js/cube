DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS targets CASCADE;
DROP TABLE IF EXISTS employee_profiles CASCADE;
DROP TABLE IF EXISTS employees CASCADE;

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department TEXT NOT NULL,
    manager_id INTEGER REFERENCES employees(id)
);

CREATE TABLE employee_profiles (
    id INTEGER PRIMARY KEY,
    employee_id INTEGER NOT NULL UNIQUE REFERENCES employees(id),
    email TEXT NOT NULL,
    salary NUMERIC(10, 2) NOT NULL
);

CREATE TABLE targets (
    id INTEGER PRIMARY KEY,
    region TEXT NOT NULL,
    category TEXT NOT NULL,
    target_amount NUMERIC(10, 2) NOT NULL,
    UNIQUE (region, category)
);

CREATE TABLE sales (
    id INTEGER PRIMARY KEY,
    region TEXT NOT NULL,
    category TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL
);

INSERT INTO employees (id, name, department, manager_id) VALUES
    (1, 'Alice',   'Engineering', NULL),
    (2, 'Bob',     'Engineering', 1),
    (3, 'Charlie', 'Sales',       1),
    (4, 'Diana',   'Sales',       3),
    (5, 'Eve',     'Engineering', 2);

INSERT INTO employee_profiles (id, employee_id, email, salary) VALUES
    (1, 1, 'alice@co.com',   150000.00),
    (2, 2, 'bob@co.com',     120000.00),
    (3, 3, 'charlie@co.com', 130000.00),
    (4, 4, 'diana@co.com',   100000.00),
    (5, 5, 'eve@co.com',     110000.00);

INSERT INTO targets (id, region, category, target_amount) VALUES
    (1, 'East', 'Electronics', 300.00),
    (2, 'East', 'Clothing',    100.00),
    (3, 'West', 'Electronics', 250.00),
    (4, 'West', 'Clothing',    150.00);

INSERT INTO sales (id, region, category, amount) VALUES
    (1, 'East', 'Electronics', 100.00),
    (2, 'East', 'Clothing',     50.00),
    (3, 'West', 'Electronics', 200.00),
    (4, 'West', 'Clothing',     75.00),
    (5, 'East', 'Electronics', 150.00);
