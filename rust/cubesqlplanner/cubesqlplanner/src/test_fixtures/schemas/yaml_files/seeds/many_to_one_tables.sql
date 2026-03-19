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

INSERT INTO many_to_one_child (id, dim, test_dim, val) VALUES
    (1, 'child_a', 'ct_x', 100.00),
    (2, 'child_b', 'ct_y', 200.00);

-- Multiple roots per child (many_to_one)
-- child_id=1: roots 1,2,3 → root vals 10,20,30 → sum=60, avg=20
-- child_id=2: roots 4,5   → root vals 40,50    → sum=90, avg=45
INSERT INTO many_to_one_root (id, child_id, dim, test_dim, val) VALUES
    (1, 1, 'root_a', 'rt_x', 10.00),
    (2, 1, 'root_b', 'rt_x', 20.00),
    (3, 1, 'root_c', 'rt_y', 30.00),
    (4, 2, 'root_d', 'rt_y', 40.00),
    (5, 2, 'root_e', 'rt_z', 50.00);
