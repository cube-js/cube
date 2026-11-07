-- Runs once on first container start. A pre-aggregation build drops every table in its
-- schema that it doesn't know, so pre-aggregations get a schema apart from the fixtures.
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE USER test_preaggs NO AUTHENTICATION DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
GRANT CREATE ANY TABLE, DROP ANY TABLE, SELECT ANY TABLE TO test;
