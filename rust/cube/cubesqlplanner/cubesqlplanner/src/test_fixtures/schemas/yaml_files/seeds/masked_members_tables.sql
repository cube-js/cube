DROP TABLE IF EXISTS workers CASCADE;

CREATE TABLE workers (
    id INTEGER PRIMARY KEY,
    full_name TEXT NOT NULL,
    department TEXT NOT NULL,
    gender TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO workers (id, full_name, department, gender, created_at) VALUES
    (1, 'Alice', 'eng', 'F', '2024-01-01 10:00:00'),
    (2, 'Bob', 'eng', 'M', '2024-01-02 11:00:00'),
    (3, 'Carol', 'ops', 'F', '2024-01-02 12:00:00');
