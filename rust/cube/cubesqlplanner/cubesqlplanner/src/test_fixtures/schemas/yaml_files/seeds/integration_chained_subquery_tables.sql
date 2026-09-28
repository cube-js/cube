DROP TABLE IF EXISTS cards CASCADE;
DROP TABLE IF EXISTS visitor_checkins CASCADE;
DROP TABLE IF EXISTS visitors CASCADE;

CREATE TABLE visitors (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL
);

CREATE TABLE visitor_checkins (
    id INTEGER PRIMARY KEY,
    visitor_id INTEGER NOT NULL REFERENCES visitors(id)
);

CREATE TABLE cards (
    id INTEGER PRIMARY KEY,
    visitor_checkin_id INTEGER NOT NULL REFERENCES visitor_checkins(id)
);

-- visitors: 1 'some', 2 'google', 3 'some', 4 'google'
INSERT INTO visitors (id, source) VALUES
    (1, 'some'),
    (2, 'google'),
    (3, 'some'),
    (4, 'google');

-- visitor_checkins: visitor 1 has 1 checkin, 2 has 1, 3 has 1, 4 has 1
INSERT INTO visitor_checkins (id, visitor_id) VALUES
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 4);

-- cards per checkin: 1->0 (none), 2->1, 3->0 (none), 4->2
INSERT INTO cards (id, visitor_checkin_id) VALUES
    (1, 2),
    (2, 4),
    (3, 4);
