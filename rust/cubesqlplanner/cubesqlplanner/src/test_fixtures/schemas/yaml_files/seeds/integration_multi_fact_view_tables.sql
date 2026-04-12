DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS impressions CASCADE;
DROP TABLE IF EXISTS periods CASCADE;

CREATE TABLE periods (
    date_id INTEGER PRIMARY KEY,
    month_name TEXT NOT NULL
);

CREATE TABLE sales (
    id INTEGER PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES periods(date_id),
    category TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL
);

CREATE TABLE impressions (
    id INTEGER PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES periods(date_id),
    channel TEXT NOT NULL,
    clicks INTEGER NOT NULL
);

INSERT INTO periods (date_id, month_name) VALUES
    (1, 'January'),
    (2, 'February'),
    (3, 'March');

INSERT INTO sales (id, date_id, category, amount) VALUES
    (1, 1, 'electronics', 100.00),
    (2, 1, 'clothing',     50.00),
    (3, 2, 'electronics', 200.00),
    (4, 2, 'electronics', 150.00),
    (5, 3, 'clothing',     75.00);

INSERT INTO impressions (id, date_id, channel, clicks) VALUES
    (1, 1, 'search',  10),
    (2, 1, 'social',  20),
    (3, 2, 'search',  30),
    (4, 3, 'social',  15),
    (5, 3, 'search',  25);
