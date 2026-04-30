DROP TABLE IF EXISTS visitors CASCADE;

CREATE TABLE visitors (
    id INTEGER PRIMARY KEY,
    amount INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    status INTEGER NOT NULL,
    source TEXT,
    latitude DECIMAL,
    longitude DECIMAL
);

INSERT INTO visitors (id, amount, created_at, updated_at, status, source, latitude, longitude) VALUES
    (1, 100, '2017-01-03', '2017-01-30', 1, 'some', 120.120, 40.60),
    (2, 200, '2017-01-05', '2017-01-15', 1, 'some', 120.120, 58.60),
    (3, 300, '2017-01-06', '2017-01-20', 2, 'google', 120.120, 70.60),
    (4, 400, '2017-01-07', '2017-01-25', 2, NULL, 120.120, 10.60),
    (5, 500, '2017-01-07', '2017-01-25', 2, NULL, 120.120, 58.10),
    (6, 500, '2016-09-07', '2016-09-07', 2, NULL, 120.120, 58.10);
