import { MaterializeDriver } from '../src';

// MaterializeDriver replaces PostgresDriver's prepareConnection to skip the
// statement_timeout Materialize does not support, so the preamble call in the
// override is the only thing keeping it on both the query and stream paths.
const driverWith = (preamble?: string) => {
  const driver = Object.create(MaterializeDriver.prototype) as any;
  const executed: string[] = [];

  const conn = {
    query: async (statement: unknown) => {
      if (typeof statement === 'string') {
        executed.push(statement);
      }
      return { rows: [], fields: [] };
    },
  };

  driver.config = { storeTimezone: 'UTC' };
  driver.sqlPreamble = () => preamble;

  return { driver, executed, conn };
};

describe('MaterializeDriver sql preamble', () => {
  afterEach(() => {
    delete process.env.CUBEJS_DB_MATERIALIZE_CLUSTER;
  });

  test('runs the preamble after the session settings it does keep', async () => {
    const { driver, executed, conn } = driverWith('SET a = 1');

    await driver.prepareConnection(conn, {});

    expect(executed).toEqual(['SET TIME ZONE \'UTC\'', 'SET a = 1']);
  });

  test('runs the preamble after the cluster selection', async () => {
    process.env.CUBEJS_DB_MATERIALIZE_CLUSTER = 'analytics';
    const { driver, executed, conn } = driverWith('SET a = 1');

    await driver.prepareConnection(conn, {});

    expect(executed.indexOf('SET a = 1')).toBeGreaterThan(executed.indexOf('SET CLUSTER TO analytics'));
  });

  test('runs nothing extra when no preamble is configured', async () => {
    const { driver, executed, conn } = driverWith(undefined);

    await driver.prepareConnection(conn, {});

    expect(executed).toEqual(['SET TIME ZONE \'UTC\'']);
  });
});
