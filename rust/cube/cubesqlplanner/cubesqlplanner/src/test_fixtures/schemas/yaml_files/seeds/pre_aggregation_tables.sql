DROP TABLE IF EXISTS visitor_checkins CASCADE;
DROP TABLE IF EXISTS visitors CASCADE;

CREATE TABLE visitors (
    id INTEGER PRIMARY KEY,
    source TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE visitor_checkins (
    id INTEGER PRIMARY KEY,
    visitor_id INTEGER NOT NULL REFERENCES visitors(id),
    source TEXT,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO visitors (id, source, created_at) VALUES
    -- Two google visitors on the same day, different times
    (1, 'google',  '2025-01-10 08:00:00'),
    (2, 'google',  '2025-01-10 23:59:59'),
    -- Google on next day — verifies day boundary
    (3, 'google',  '2025-01-11 00:00:00'),
    -- Twitter across months
    (4, 'twitter', '2025-01-31 23:59:59'),
    (5, 'twitter', '2025-02-01 00:00:00'),
    -- Organic single entry
    (6, 'organic', '2025-02-15 12:00:00'),
    -- NULL source — edge case
    (7, NULL,      '2025-02-15 12:00:00'),
    -- Far apart dates — checks wide range aggregation
    (8, 'google',  '2024-12-31 23:59:59'),
    (9, 'google',  '2025-03-01 00:00:01'),
    -- Duplicate source+date — should aggregate correctly
    (10, 'organic', '2025-02-15 12:00:00');

INSERT INTO visitor_checkins (id, visitor_id, source, created_at) VALUES
    -- Multiple checkins per visitor
    (1, 1, 'web',    '2025-01-10 09:00:00'),
    (2, 1, 'web',    '2025-01-10 15:00:00'),
    (3, 1, 'mobile', '2025-01-11 10:00:00'),
    -- Checkins crossing day boundary
    (4, 4, 'web',    '2025-01-31 23:30:00'),
    (5, 4, 'web',    '2025-02-01 00:30:00'),
    -- Checkin on same day as visitor but different source
    (6, 5, 'mobile', '2025-02-01 08:00:00'),
    (7, 6, 'web',    '2025-02-15 13:00:00'),
    -- Visitor with NULL source also has checkins
    (8, 7, 'web',    '2025-02-16 10:00:00'),
    -- Late checkin far from visitor creation
    (9, 8, 'mobile', '2025-03-01 12:00:00');
