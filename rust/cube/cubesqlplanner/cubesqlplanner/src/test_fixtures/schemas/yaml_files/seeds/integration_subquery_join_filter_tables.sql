DROP TABLE IF EXISTS sq_orders CASCADE;
DROP TABLE IF EXISTS sq_labels CASCADE;

CREATE TABLE sq_orders (
    id INTEGER PRIMARY KEY,
    amount INTEGER NOT NULL
);

CREATE TABLE sq_labels (
    amount INTEGER PRIMARY KEY,
    label TEXT NOT NULL
);

INSERT INTO sq_orders (id, amount) VALUES
    (1, 10),
    (2, 20),
    (3, 30);

INSERT INTO sq_labels (amount, label) VALUES
    (10, 'ten'),
    (20, 'twenty'),
    (40, 'forty');
