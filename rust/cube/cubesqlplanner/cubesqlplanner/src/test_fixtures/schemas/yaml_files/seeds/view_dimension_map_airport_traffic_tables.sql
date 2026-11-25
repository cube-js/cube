DROP TABLE IF EXISTS vdo_airports CASCADE;
DROP TABLE IF EXISTS vdo_flights CASCADE;

CREATE TABLE vdo_airports (
    code TEXT PRIMARY KEY,
    city TEXT NOT NULL,
    country TEXT NOT NULL
);

INSERT INTO vdo_airports (code, city, country) VALUES
    ('SFO', 'San Francisco', 'US'),
    ('JFK', 'New York', 'US'),
    ('LHR', 'London', 'GB');

CREATE TABLE vdo_flights (
    id INTEGER PRIMARY KEY,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    passengers INTEGER NOT NULL
);

-- Departures: SFO 3 (390 passengers), JFK 2 (330), LHR 1 (120).
-- Arrivals: SFO 2 (270), JFK 2 (190), LHR 2 (380).
INSERT INTO vdo_flights (id, origin, destination, passengers) VALUES
    (1, 'SFO', 'JFK', 100),
    (2, 'SFO', 'LHR', 200),
    (3, 'JFK', 'SFO', 150),
    (4, 'LHR', 'SFO', 120),
    (5, 'JFK', 'LHR', 180),
    (6, 'SFO', 'JFK', 90);
