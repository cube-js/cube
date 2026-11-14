DROP TABLE IF EXISTS activity CASCADE;

CREATE TABLE activity (
    id INTEGER PRIMARY KEY,
    player VARCHAR(50) NOT NULL,
    theowin INTEGER NOT NULL
);

INSERT INTO activity (id, player, theowin) VALUES
    (1, 'p1', 100),
    (2, 'p2', 10),
    (3, 'p2', 5);
