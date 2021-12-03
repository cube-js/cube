CREATE TABLE visitors (id INT, amount INT, created_at TIMESTAMP, updated_at TIMESTAMP, status INT, source TEXT, latitude DECIMAL, longitude DECIMAL);
CREATE TABLE visitor_checkins (id INT, visitor_id INT, created_at TIMESTAMP, source TEXT);
CREATE TABLE cards (id INT, visitor_id INT, visitor_checkin_id INT);

INSERT INTO
    visitors
    (id, amount, created_at, updated_at, status, source, latitude, longitude) VALUES
    (1, 100, '2017-01-03', '2017-01-30', 1, 'some', 120.120, 40.60),
    (2, 200, '2017-01-05', '2017-01-15', 1, 'some', 120.120, 58.60),
    (3, 300, '2017-01-06', '2017-01-20', 2, 'google', 120.120, 70.60),
    (4, 400, '2017-01-07', '2017-01-25', 2, NULL, 120.120, 10.60),
    (5, 500, '2017-01-07', '2017-01-25', 2, NULL, 120.120, 58.10),
    (6, 500, '2016-09-07', '2016-09-07', 2, NULL, 120.120, 58.10);

INSERT INTO
    visitor_checkins
    (id, visitor_id, created_at, source) VALUES
    (1, 1, '2017-01-03', NULL),
    (2, 1, '2017-01-04', NULL),
    (3, 1, '2017-01-05', 'google'),
    (4, 2, '2017-01-05', NULL),
    (5, 2, '2017-01-05', NULL),
    (6, 3, '2017-01-06', NULL);

INSERT INTO
    cards
    (id, visitor_id, visitor_checkin_id) VALUES
    (1, 1, 1),
    (2, 1, 2),
    (3, 3, 6);
