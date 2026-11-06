DROP TABLE IF EXISTS rjmu_delivery CASCADE;
DROP TABLE IF EXISTS rjmu_tactics CASCADE;
DROP TABLE IF EXISTS rjmu_client_mapping CASCADE;

CREATE TABLE rjmu_delivery (
    row_key INTEGER PRIMARY KEY,
    tactic_id INTEGER NOT NULL,
    day TIMESTAMP NOT NULL,
    impressions INTEGER NOT NULL,
    net_spend NUMERIC(10,2) NOT NULL
);

CREATE TABLE rjmu_tactics (
    tactic_id INTEGER PRIMARY KEY,
    brand_id VARCHAR(10) NOT NULL,
    margin NUMERIC(4,2) NOT NULL
);

CREATE TABLE rjmu_client_mapping (
    brand_id VARCHAR(10) PRIMARY KEY,
    client_id VARCHAR(10) NOT NULL
);

INSERT INTO rjmu_delivery (row_key, tactic_id, day, impressions, net_spend) VALUES
    (1, 1, '2026-01-05', 100, 80.00),
    (2, 1, '2026-02-10',  50, 40.00),
    (3, 2, '2026-01-20', 200, 90.00),
    (4, 3, '2026-02-15', 400, 50.00),
    (5, 3, '2026-03-01', 300, 100.00);

INSERT INTO rjmu_tactics (tactic_id, brand_id, margin) VALUES
    (1, 'b1', 0.20),
    (2, 'b1', 0.10),
    (3, 'b2', 0.50);

INSERT INTO rjmu_client_mapping (brand_id, client_id) VALUES
    ('b1', 'c1'),
    ('b2', 'c2');
