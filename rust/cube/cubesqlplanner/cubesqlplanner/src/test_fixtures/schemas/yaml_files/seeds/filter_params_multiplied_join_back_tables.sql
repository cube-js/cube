-- Orders of two tenants, some outside the reported period, each carrying
-- several tags so that the tag dimension multiplies the fact rows.

DROP TABLE IF EXISTS fpmjb_order_tags CASCADE;
DROP TABLE IF EXISTS fpmjb_orders CASCADE;
DROP TABLE IF EXISTS fpmjb_users CASCADE;

CREATE TABLE fpmjb_users (
    id     int,
    is_vip boolean
);

INSERT INTO fpmjb_users (id, is_vip) VALUES
    (1, true),
    (2, false),
    (3, true),
    (4, true);

CREATE TABLE fpmjb_orders (
    id         int,
    tenant_id  text,
    user_id    int,
    created_at timestamp,
    amount     numeric
);

INSERT INTO fpmjb_orders (id, tenant_id, user_id, created_at, amount) VALUES
    (1, 't1', 1, TIMESTAMP '2026-08-01 10:00:00', 100),
    (2, 't1', 1, TIMESTAMP '2026-08-02 10:00:00', 200),
    (3, 't1', 2, TIMESTAMP '2026-08-03 10:00:00', 300),
    (4, 't1', 3, TIMESTAMP '2026-06-01 10:00:00', 400),
    (5, 't2', 4, TIMESTAMP '2026-08-04 10:00:00', 500);

CREATE TABLE fpmjb_order_tags (
    id       int,
    order_id int,
    tag      text
);

INSERT INTO fpmjb_order_tags (id, order_id, tag) VALUES
    (1, 1, 'a'),
    (2, 1, 'b'),
    (3, 2, 'a'),
    (4, 3, 'b'),
    (5, 4, 'a'),
    (6, 5, 'a');
