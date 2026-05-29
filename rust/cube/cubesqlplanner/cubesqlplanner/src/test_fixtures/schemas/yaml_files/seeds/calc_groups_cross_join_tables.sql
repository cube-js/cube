DROP TABLE IF EXISTS calc_groups_source_a;
CREATE TABLE calc_groups_source_a (
    id INT,
    product_category TEXT,
    created_at TIMESTAMPTZ,
    price_usd NUMERIC,
    price_eur NUMERIC
);
INSERT INTO calc_groups_source_a VALUES
    (10, 'some category',   '2022-01-12T20:00:00.000Z', 100, 0),
    (11, 'some category',   '2022-01-14T20:00:00.000Z', 500, 0),
    (12, 'some category A', '2022-02-12T20:00:00.000Z', 200, 0),
    (13, 'some category A', '2022-03-14T20:00:00.000Z', 300, 0);

DROP TABLE IF EXISTS calc_groups_source_b;
CREATE TABLE calc_groups_source_b (
    id INT,
    product_category TEXT,
    created_at TIMESTAMPTZ,
    price_usd NUMERIC,
    price_eur NUMERIC
);
INSERT INTO calc_groups_source_b VALUES
    (10, 'some category',   '2022-01-12T20:00:00.000Z', 0, 100),
    (11, 'some category',   '2022-02-12T20:00:00.000Z', 0, 500),
    (12, 'some category B', '2022-02-15T20:00:00.000Z', 0, 200),
    (13, 'some category B', '2022-03-12T20:00:00.000Z', 0, 300),
    (14, 'some category B', '2022-04-12T20:00:00.000Z', 0, 300);
