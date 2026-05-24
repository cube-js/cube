DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    age INTEGER NOT NULL,
    payments NUMERIC(10, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO customers (id, name, city, age, payments, created_at) VALUES
    (1, 'Alice', 'New York', 30, 500.00, '2025-01-10 10:00:00'),
    (2, 'Bob', 'New York', 25, 300.00, '2025-01-15 11:00:00'),
    (3, 'Charlie', 'Boston', 35, 700.00, '2025-02-01 09:00:00'),
    (4, 'Diana', 'Boston', 28, 150.00, '2025-02-10 14:00:00');

INSERT INTO orders (id, customer_id, status, priority, amount, created_at) VALUES
    (1, 1, 'completed', 'high', 100.00, '2025-03-01 08:00:00'),
    (2, 1, 'completed', 'low', 50.00, '2025-03-01 12:00:00'),
    (3, 2, 'pending', 'high', 200.00, '2025-03-02 09:00:00'),
    (4, 3, 'completed', 'medium', 75.00, '2025-03-03 10:00:00'),
    (5, 3, 'cancelled', 'low', 25.00, '2025-03-03 15:00:00'),
    (6, 4, 'pending', 'high', 300.00, '2025-03-04 11:00:00');
