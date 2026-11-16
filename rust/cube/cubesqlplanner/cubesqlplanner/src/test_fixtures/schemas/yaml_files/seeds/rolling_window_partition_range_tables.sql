DROP TABLE IF EXISTS prr_events CASCADE;
DROP TABLE IF EXISTS prr_sites CASCADE;
DROP TABLE IF EXISTS prr_projects CASCADE;

CREATE TABLE prr_sites (
    id TEXT PRIMARY KEY
);

CREATE TABLE prr_projects (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL
);

CREATE TABLE prr_events (
    id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    site_id TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO prr_sites (id) VALUES ('s1'), ('s2');

INSERT INTO prr_projects (id, site_id) VALUES
    ('p1', 's1'),
    ('p2', 's1'),
    ('p3', 's2');

INSERT INTO prr_events (id, status, site_id, created_at) VALUES
    (1, 'new',    's1', '2024-01-01 10:00:00'),
    (2, 'active', 's1', '2024-01-01 12:00:00'),
    (3, 'active', 's2', '2024-01-02 09:00:00'),
    (4, 'closed', 's1', '2024-01-03 15:00:00'),
    (5, 'new',    's2', '2024-01-03 16:00:00'),
    (6, 'active', 's2', '2024-01-05 08:00:00');
