DROP TABLE IF EXISTS switch_collision_facts;
CREATE TABLE switch_collision_facts (
    id INT,
    kind TEXT,
    amount NUMERIC
);
INSERT INTO switch_collision_facts VALUES
    (1, 'a', 10),
    (2, 'a', 20),
    (3, 'b', 100);
