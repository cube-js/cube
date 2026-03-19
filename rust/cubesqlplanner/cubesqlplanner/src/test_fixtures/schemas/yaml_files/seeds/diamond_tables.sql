DROP TABLE IF EXISTS table_a CASCADE;
DROP TABLE IF EXISTS table_b CASCADE;
DROP TABLE IF EXISTS table_c CASCADE;

CREATE TABLE table_c (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,
    description TEXT,
    item_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE table_b (
    id INTEGER PRIMARY KEY,
    c_id INTEGER NOT NULL REFERENCES table_c(id),
    category TEXT NOT NULL,
    status TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE table_a (
    id INTEGER PRIMARY KEY,
    b_id INTEGER NOT NULL REFERENCES table_b(id),
    c_id INTEGER NOT NULL REFERENCES table_c(id),
    name TEXT NOT NULL,
    value NUMERIC(10, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO table_c (id, code, description, item_count) VALUES
    (1, 'C001', 'Category Alpha', 10),
    (2, 'C002', 'Category Beta', 20),
    (3, 'C003', 'Category Gamma', 30);

INSERT INTO table_b (id, c_id, category, status, amount) VALUES
    (1, 1, 'electronics', 'active', 100.00),
    (2, 1, 'clothing', 'active', 200.00),
    (3, 2, 'electronics', 'inactive', 150.00),
    (4, 3, 'food', 'active', 50.00);

INSERT INTO table_a (id, b_id, c_id, name, value) VALUES
    (1, 1, 1, 'Item A1', 10.50),
    (2, 2, 1, 'Item A2', 20.00),
    (3, 3, 2, 'Item A3', 30.75),
    (4, 4, 2, 'Item A4', 15.25),
    (5, 1, 3, 'Item A5', 40.00);
