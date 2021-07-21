import { StartedTestContainer } from 'testcontainers';

import { MySqlDriver } from '../src/MySqlDriver';

export const createDriver = (c: StartedTestContainer) => new MySqlDriver({
  host: c.getHost(),
  user: 'root',
  password: process.env.TEST_DB_PASSWORD || 'Test1test',
  port: c.getMappedPort(3306),
  database: 'mysql',
});
