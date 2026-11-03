DROP TABLE IF EXISTS rcd_sessions CASCADE;

CREATE TABLE rcd_sessions (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

-- Users recur across days and the daily distinct counts repeat, so distinct
-- counts over stored daily counts diverge from distinct users in the window.
INSERT INTO rcd_sessions (id, user_id, amount, created_at) VALUES
    (1, 1, 10.00, '2024-01-01 12:00:00'),
    (2, 2, 20.00, '2024-01-01 13:00:00'),
    (3, 1, 30.00, '2024-01-02 12:00:00'),
    (4, 3, 40.00, '2024-01-02 13:00:00'),
    (5, 4, 50.00, '2024-01-03 12:00:00'),
    (6, 5, 60.00, '2024-01-03 13:00:00'),
    (7, 5, 70.00, '2024-01-03 14:00:00'),
    (8, 1, 80.00, '2024-01-08 12:00:00');
