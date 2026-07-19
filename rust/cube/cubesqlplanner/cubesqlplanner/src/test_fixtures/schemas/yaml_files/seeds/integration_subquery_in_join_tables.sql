DROP TABLE IF EXISTS c CASCADE;
DROP TABLE IF EXISTS b CASCADE;
DROP TABLE IF EXISTS a CASCADE;

CREATE TABLE a (
    id INTEGER PRIMARY KEY,
    foo_id INTEGER NOT NULL
);

CREATE TABLE b (
    id INTEGER PRIMARY KEY,
    foo_id INTEGER NOT NULL,
    bar_id INTEGER NOT NULL
);

CREATE TABLE c (
    id INTEGER PRIMARY KEY,
    bar_id INTEGER NOT NULL,
    important_value NUMERIC(10, 2) NOT NULL
);

INSERT INTO a (id, foo_id) VALUES
    (79, 1),
    (80, 2),
    (81, 3),
    (82, 4),
    (83, 5),
    (84, 6);

INSERT INTO b (id, foo_id, bar_id) VALUES
    (100, 1, 450),
    (101, 2, 450),
    (102, 3, 452),
    (103, 4, 452),
    (104, 5, 478);

INSERT INTO c (id, bar_id, important_value) VALUES
    (789, 450, 0.2),
    (790, 450, 0.3),
    (791, 452, 5.6),
    (792, 452, 5.6),
    (793, 478, 38.0),
    (794, 478, 43.5);
