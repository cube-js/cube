DROP TABLE IF EXISTS rjk_facts CASCADE;
DROP TABLE IF EXISTS rjk_dates CASCADE;

CREATE TABLE rjk_facts (
    id     INTEGER PRIMARY KEY,
    day    TIMESTAMP NOT NULL,
    amount NUMERIC   NOT NULL
);

CREATE TABLE rjk_dates (
    day           TIMESTAMP PRIMARY KEY,
    quarter_label TEXT      NOT NULL
);

INSERT INTO rjk_facts (id, day, amount) VALUES
    (1, '2026-01-01', 10),
    (2, '2026-01-01', 5),
    (3, '2026-02-15', 7),
    (4, '2026-04-02', 3);

INSERT INTO rjk_dates (day, quarter_label) VALUES
    ('2026-01-01', 'Q1'),
    ('2026-02-15', 'Q1'),
    ('2026-04-02', 'Q2');
