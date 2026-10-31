DROP TABLE IF EXISTS msk_items CASCADE;
DROP TABLE IF EXISTS msk_owners CASCADE;

CREATE TABLE msk_owners (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO msk_owners (id, name) VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Carol');

CREATE TABLE msk_items (
    id INTEGER PRIMARY KEY,
    owner_id INTEGER NOT NULL REFERENCES msk_owners(id)
);

-- Carol owns no items, so her row reaches items with a NULL id.
INSERT INTO msk_items (id, owner_id) VALUES
    (10, 1),
    (11, 1),
    (20, 2);
