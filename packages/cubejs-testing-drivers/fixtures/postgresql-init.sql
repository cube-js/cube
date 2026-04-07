-- Create a separate PostgreSQL user for pre-aggregation queries.
-- This script runs automatically via /docker-entrypoint-initdb.d/ on first container start.
CREATE USER test_preagg WITH PASSWORD 'test_preagg';
GRANT CONNECT ON DATABASE test TO test_preagg;
GRANT USAGE ON SCHEMA public TO test_preagg;
-- Grant SELECT on all future tables created by the 'test' user in the public schema,
-- so test_preagg can read fixture tables created during test setup.
ALTER DEFAULT PRIVILEGES FOR ROLE test IN SCHEMA public GRANT SELECT ON TABLES TO test_preagg;
