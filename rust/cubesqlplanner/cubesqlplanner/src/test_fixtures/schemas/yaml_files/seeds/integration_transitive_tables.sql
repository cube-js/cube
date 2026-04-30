DROP TABLE IF EXISTS t_a CASCADE;
DROP TABLE IF EXISTS t_b CASCADE;
DROP TABLE IF EXISTS t_c CASCADE;
DROP TABLE IF EXISTS t_d CASCADE;

CREATE TABLE t_d (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL
);

CREATE TABLE t_c (
    id INTEGER PRIMARY KEY,
    d_id INTEGER NOT NULL REFERENCES t_d(id)
);

CREATE TABLE t_b (
    id INTEGER PRIMARY KEY,
    c_id INTEGER NOT NULL REFERENCES t_c(id)
);

CREATE TABLE t_a (
    id INTEGER PRIMARY KEY,
    b_id INTEGER NOT NULL REFERENCES t_b(id),
    value NUMERIC(10,2) NOT NULL
);

INSERT INTO t_d (id, label) VALUES
    (1, 'X'),
    (2, 'Y');

INSERT INTO t_c (id, d_id) VALUES
    (1, 1),
    (2, 2);

INSERT INTO t_b (id, c_id) VALUES
    (1, 1),
    (2, 2);

-- t_a: id=1,2 -> b_id=1 -> c_id=1 -> d_id=1 (label=X), value=10,20
--       id=3 -> b_id=2 -> c_id=2 -> d_id=2 (label=Y), value=30
INSERT INTO t_a (id, b_id, value) VALUES
    (1, 1, 10.00),
    (2, 1, 20.00),
    (3, 2, 30.00);
