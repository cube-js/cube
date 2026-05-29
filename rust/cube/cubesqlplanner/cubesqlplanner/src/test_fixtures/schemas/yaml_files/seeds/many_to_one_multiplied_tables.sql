DROP TABLE IF EXISTS many_to_one_root CASCADE;
DROP TABLE IF EXISTS many_to_one_child CASCADE;

CREATE TABLE many_to_one_child (
    id INTEGER PRIMARY KEY,
    dim TEXT NOT NULL,
    test_dim TEXT NOT NULL,
    val NUMERIC(10, 2) NOT NULL DEFAULT 0
);

CREATE TABLE many_to_one_root (
    id INTEGER PRIMARY KEY,
    child_id INTEGER NOT NULL REFERENCES many_to_one_child(id),
    dim TEXT NOT NULL,
    test_dim TEXT NOT NULL,
    val NUMERIC(10, 2) NOT NULL DEFAULT 0
);

-- Mirrors the JS member-expressions-on-views test data shape: several
-- root rows pointing to the same child, sharing `dim` values across
-- (root, child). Group `(root.dim, child.dim) = (foo, foo)` covers
-- three root rows joined to two distinct children — bringing the
-- child measure multiplication bug into view (avg without dedup gives
-- (100 + 100 + 300) / 3 = 166.66 instead of the correct
-- (100 + 300) / 2 = 200).
INSERT INTO many_to_one_child (id, dim, test_dim, val) VALUES
    (1, 'foo', 'one', 100.00),
    (2, 'foo', 'two', 300.00),
    (3, 'bar', 'three', 500.00);

INSERT INTO many_to_one_root (id, child_id, dim, test_dim, val) VALUES
    (1, 1, 'foo', 'one', 100.00),
    (2, 1, 'foo', 'two', 300.00),
    (3, 2, 'foo', 'two', 800.00),
    (4, 3, 'bar', 'three', 500.00);
