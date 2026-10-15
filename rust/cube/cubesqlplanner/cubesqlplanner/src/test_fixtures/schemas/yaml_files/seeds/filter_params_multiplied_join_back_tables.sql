-- Orders of two tenants, some outside the reported period, each carrying
-- several tags so that the tag dimension multiplies the fact rows.

DROP TABLE IF EXISTS fpmjb_order_tags CASCADE;
DROP TABLE IF EXISTS fpmjb_orders CASCADE;
DROP TABLE IF EXISTS fpmjb_users CASCADE;

-- `signup_at` is the users' own date, unrelated to any order's `created_at`,
-- so a predicate over it is not implied by the filters the keys side applies.
-- User 1 signed up before the reported period, the rest inside it.
CREATE TABLE fpmjb_users (
    id        int,
    is_vip    boolean,
    signup_at timestamp
);

INSERT INTO fpmjb_users (id, is_vip, signup_at) VALUES
    (1, true,  TIMESTAMP '2026-01-15 09:00:00'),
    (2, false, TIMESTAMP '2026-08-01 09:00:00'),
    (3, true,  TIMESTAMP '2026-08-02 09:00:00'),
    (4, true,  TIMESTAMP '2026-08-03 09:00:00');

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
    (5, 't2', 4, TIMESTAMP '2026-08-04 10:00:00', 500),
    (6, 't1', 3, TIMESTAMP '2026-08-05 10:00:00', 700);

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
    (6, 5, 'a'),
    (7, 6, 'b');
