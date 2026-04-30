DROP TABLE IF EXISTS stores CASCADE;

CREATE TABLE stores (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    latitude NUMERIC(10,6),
    longitude NUMERIC(10,6),
    revenue NUMERIC(10,2)
);

INSERT INTO stores (id, name, latitude, longitude, revenue) VALUES
    (1, 'Store A', 40.712776, -74.005974, 1000.00),
    (2, 'Store B', 34.052235, -118.243683, 2000.00),
    (3, 'Store C', 40.730610, -73.935242, 1500.00);
