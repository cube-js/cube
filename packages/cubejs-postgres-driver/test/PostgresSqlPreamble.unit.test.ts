import { PostgresDriver } from '../src';

// No Postgres server involved: the point is which statements reach the
// connection, and on which paths. The pg pool and client are stubbed.
const driverWith = (preamble?: string) => {
  const driver = Object.create(PostgresDriver.prototype) as any;
  const executed: string[] = [];

  const conn = {
    query: async (statement: unknown) => {
      // The stream path passes a QueryStream object rather than SQL text; only
      // the preamble and session settings arrive as strings.
      if (typeof statement === 'string') {
        executed.push(statement);
        return { rows: [], fields: [] };
      }

      // What the driver expects back from a QueryStream query.
      return { fields: async () => [] };
    },
    release: async () => { /* stubbed */ },
  };

  driver.config = { storeTimezone: 'UTC', executionTimeout: 600 };
  driver.sqlPreamble = () => preamble;
  driver.loadUserDefinedTypes = async () => { /* no type discovery in the harness */ };
  driver.pool = {
    acquire: async () => conn,
    release: async () => { /* stubbed */ },
    _factory: { destroy: async () => { /* stubbed */ } },
  };

  return { driver, executed, conn };
};

const PREAMBLE = 'CREATE TEMP TABLE seen (id int)';

describe('PostgresDriver sql preamble', () => {
  test('runs the preamble when a connection is prepared', async () => {
    const { driver, executed, conn } = driverWith(PREAMBLE);

    await driver.prepareConnection(conn, { executionTimeout: 600 });

    expect(executed).toContain(PREAMBLE);
  });

  test('runs the preamble after the session settings, so a user can override them', async () => {
    const { driver, executed, conn } = driverWith('SET TIME ZONE \'Europe/Berlin\'');

    await driver.prepareConnection(conn, { executionTimeout: 600 });

    const builtIn = executed.findIndex(s => s.startsWith('SET TIME ZONE \'UTC\''));
    const fromPreamble = executed.lastIndexOf('SET TIME ZONE \'Europe/Berlin\'');

    expect(builtIn).toBeGreaterThanOrEqual(0);
    expect(fromPreamble).toBeGreaterThan(builtIn);
  });

  test('runs nothing extra when no preamble is configured', async () => {
    const { driver, executed, conn } = driverWith(undefined);

    await driver.prepareConnection(conn, { executionTimeout: 600 });

    expect(executed).toEqual([
      'SET TIME ZONE \'UTC\'',
      'SET statement_timeout TO 600',
    ]);
  });

  test('runs each statement of a multi-statement preamble', async () => {
    const { driver, executed, conn } = driverWith('SET a = 1; SET b = 2');

    await driver.prepareConnection(conn, { executionTimeout: 600 });

    expect(executed).toContain('SET a = 1');
    expect(executed).toContain('SET b = 2');
  });

  // The stream path acquires its own connection. It shares prepareConnection
  // with the query path, so no stream-specific code carries the preamble — which
  // is exactly why a regression here would otherwise be silent.
  test('runs the preamble on the stream path', async () => {
    const { driver, executed } = driverWith(PREAMBLE);

    await driver.stream('SELECT 1', [], { highWaterMark: 100 });

    expect(executed).toContain(PREAMBLE);
  });

  // A pooled connection may already have had the preamble run on it, so
  // re-execution has to be tolerated — otherwise the second query on a reused
  // connection fails on "already exists", which is the feature's headline use
  // case (a preamble that CREATEs something).
  test('tolerates a preamble statement that was already applied', async () => {
    const { driver, conn } = driverWith(PREAMBLE);
    conn.query = async (statement: unknown) => {
      if (statement === PREAMBLE) {
        throw new Error('relation "seen" already exists');
      }
      return { rows: [], fields: [] };
    };

    await expect(driver.prepareConnection(conn, { executionTimeout: 600 })).resolves.not.toThrow();
  });

  test('still surfaces a genuine preamble error', async () => {
    const { driver, conn } = driverWith(PREAMBLE);
    conn.query = async (statement: unknown) => {
      if (statement === PREAMBLE) {
        throw new Error('permission denied for schema public');
      }
      return { rows: [], fields: [] };
    };

    await expect(driver.prepareConnection(conn, { executionTimeout: 600 }))
      .rejects.toThrow(/permission denied/);
  });
});
