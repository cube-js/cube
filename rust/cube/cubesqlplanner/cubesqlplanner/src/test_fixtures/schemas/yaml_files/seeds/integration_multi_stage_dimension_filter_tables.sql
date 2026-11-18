DROP TABLE IF EXISTS msdf_activity CASCADE;

CREATE TABLE msdf_activity (
    id INTEGER PRIMARY KEY,
    player TEXT NOT NULL,
    played_at TIMESTAMPTZ NOT NULL,
    theowin INTEGER NOT NULL
);

INSERT INTO msdf_activity (id, player, played_at, theowin) VALUES
    (1, 'p1', '2024-01-10T00:00:00Z', 100),
    (2, 'p1', '2024-02-10T00:00:00Z', 200),
    (3, 'p2', '2024-01-15T00:00:00Z',  50),
    (4, 'p2', '2024-03-15T00:00:00Z',  60);
