DROP TABLE IF EXISTS mvjp_line_items CASCADE;
DROP TABLE IF EXISTS mvjp_orders CASCADE;
DROP TABLE IF EXISTS mvjp_accounts CASCADE;
DROP TABLE IF EXISTS mvjp_customers CASCADE;

CREATE TABLE mvjp_customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    bonus INTEGER NOT NULL
);

CREATE TABLE mvjp_accounts (
    id INTEGER PRIMARY KEY,
    owner_id INTEGER NOT NULL
);

CREATE TABLE mvjp_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    amount INTEGER NOT NULL
);

CREATE TABLE mvjp_line_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    sku TEXT NOT NULL
);

-- The direct customer and the account owner differ, so the bonus names the
-- path taken: 5 + 1 along the view's path, 5 + 100 along the direct join.
INSERT INTO mvjp_customers (id, name, bonus) VALUES
    (1, 'direct', 100),
    (2, 'owner', 1);

INSERT INTO mvjp_accounts (id, owner_id) VALUES (1, 2);

INSERT INTO mvjp_orders (id, customer_id, account_id, amount) VALUES (1, 1, 1, 5);

INSERT INTO mvjp_line_items (id, order_id, sku) VALUES
    (1, 1, 'a'),
    (2, 1, 'b');
