-- This script runs automatically via /docker-entrypoint-initdb.d/ on first container start.
-- The 'test' superuser owns the database and creates fixture tables.

-- Read-only default user (cannot create tables)
CREATE USER test_readonly WITH PASSWORD 'test_readonly';
GRANT CONNECT ON DATABASE test TO test_readonly;
GRANT USAGE ON SCHEMA public TO test_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE test IN SCHEMA public GRANT SELECT ON TABLES TO test_readonly;

-- Separate read-only user for pre-aggregation queries
CREATE USER test_preagg WITH PASSWORD 'test_preagg';
GRANT CONNECT ON DATABASE test TO test_preagg;
GRANT USAGE ON SCHEMA public TO test_preagg;
ALTER DEFAULT PRIVILEGES FOR ROLE test IN SCHEMA public GRANT SELECT ON TABLES TO test_preagg;
