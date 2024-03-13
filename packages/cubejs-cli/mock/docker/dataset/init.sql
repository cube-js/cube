CREATE TABLE events (
    dt TEXT,
    id INT
);

COPY events
FROM '/docker-entrypoint-initdb.d/events.csv'
DELIMITER ','
CSV HEADER;