DROP TABLE IF EXISTS dates CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS refunds CASCADE;
DROP TABLE IF EXISTS surveys CASCADE;
DROP TABLE IF EXISTS people CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS complaints CASCADE;
DROP TABLE IF EXISTS attendances CASCADE;

CREATE TABLE dates (
    date TIMESTAMP PRIMARY KEY
);

INSERT INTO dates (date) VALUES
    ('2024-01-01'),
    ('2024-01-02'),
    ('2024-01-03');

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    event_date TIMESTAMP NOT NULL
);

INSERT INTO orders (id, status, event_date) VALUES
    (1, 'new', '2024-01-01'),
    (2, 'done', '2024-01-01'),
    (3, 'done', '2024-01-02');

CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL
);

INSERT INTO order_items (id, order_id) VALUES
    (1, 1),
    (2, 1),
    (3, 3);

CREATE TABLE refunds (
    id INTEGER PRIMARY KEY,
    event_date TIMESTAMP NOT NULL
);

INSERT INTO refunds (id, event_date) VALUES
    (1, '2024-01-02'),
    (2, '2024-01-03');

CREATE TABLE surveys (
    id INTEGER PRIMARY KEY
);

INSERT INTO surveys (id) VALUES (1);

CREATE TABLE people (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO people (id, name) VALUES
    (1, 'Ann'),
    (2, 'Bob'),
    (3, 'Cid');

CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL
);

INSERT INTO events (id, title) VALUES
    (1, 'Expo'),
    (2, 'Fair');

CREATE TABLE complaints (
    id INTEGER PRIMARY KEY,
    event_id INTEGER NOT NULL,
    complainant_id INTEGER NOT NULL,
    handler_id INTEGER NOT NULL
);

INSERT INTO complaints (id, event_id, complainant_id, handler_id) VALUES
    (1, 1, 1, 3),
    (2, 1, 1, 3),
    (3, 2, 2, 3);

CREATE TABLE attendances (
    id INTEGER PRIMARY KEY,
    event_id INTEGER NOT NULL,
    person_id INTEGER NOT NULL
);

INSERT INTO attendances (id, event_id, person_id) VALUES
    (1, 1, 1),
    (2, 1, 2),
    (3, 2, 2),
    (4, 2, 3);
