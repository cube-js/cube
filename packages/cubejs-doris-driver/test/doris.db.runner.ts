import { StartedTestContainer } from 'testcontainers';

import { DorisDriver } from '../src/DorisDriver';

/**
 * Creates a DorisDriver instance for testing.
 * Since Doris is MySQL-compatible, we can use a MySQL container for unit tests.
 */
export const createDriver = (c: StartedTestContainer) => new DorisDriver({
  host: c.getHost(),
  user: 'root',
  password: process.env.TEST_DB_PASSWORD || 'Test1test',
  port: c.getMappedPort(3306),
  database: 'mysql',
});
