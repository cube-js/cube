import { streamToArray } from '@cubejs-backend/shared';
import { DuckDBDriver } from '../../src';

// DuckDB is embedded, so the preamble actually executes here — this is the one
// driver where the feature can be verified end-to-end without a cloud account.
describe('DuckDBDriver sql preamble', () => {
  jest.setTimeout(2 * 60 * 1000);

  const drivers: DuckDBDriver[] = [];

  const driverWith = (config: Record<string, any>): DuckDBDriver => {
    const driver = new DuckDBDriver(config);
    drivers.push(driver);
    return driver;
  };

  afterEach(async () => {
    await Promise.all(drivers.splice(0).map(driver => driver.release()));
    delete process.env.CUBEJS_DB_SQL_PREAMBLE;
  });

  test('a preamble macro is visible to the query', async () => {
    const driver = driverWith({ sqlPreamble: 'CREATE MACRO double_it(x) AS x * 2' });

    expect(await driver.query('SELECT double_it(21) AS v', [])).toEqual([{ v: '42' }]);
  });

  test('a multi-statement preamble runs every statement', async () => {
    const driver = driverWith({
      sqlPreamble: 'CREATE MACRO one() AS 1; CREATE MACRO two() AS 2;',
    });

    expect(await driver.query('SELECT one() + two() AS v', [])).toEqual([{ v: '3' }]);
  });

  test('the preamble is read from the env var', async () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE MACRO from_env() AS 7';
    const driver = driverWith({});

    expect(await driver.query('SELECT from_env() AS v', [])).toEqual([{ v: '7' }]);
  });

  test('the config value wins over the env var', async () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'CREATE MACRO which() AS 1';
    const driver = driverWith({ sqlPreamble: 'CREATE MACRO which() AS 2' });

    expect(await driver.query('SELECT which() AS v', [])).toEqual([{ v: '2' }]);
  });

  const streamRows = async (driver: DuckDBDriver, query: string) => {
    const tableData = await driver.stream(query, [], { highWaterMark: 100 });

    try {
      return await streamToArray(tableData.rowStream as any);
    } finally {
      if (tableData.release) {
        await tableData.release();
      }
    }
  };

  // stream() opens its own connection rather than the one init() prepared. A
  // macro lives in the shared catalog, so it survives that on its own.
  test('a preamble macro is visible to a streamed query', async () => {
    const driver = driverWith({ sqlPreamble: 'CREATE MACRO tripled(x) AS x * 3' });

    expect(await streamRows(driver, 'SELECT tripled(5) AS v')).toEqual([{ v: '15' }]);
  });

  // A TEMP object is connection-scoped in DuckDB — unlike a macro or a SET,
  // which are database-scoped — so it does NOT survive the switch to the
  // stream's own connection. This is what the replay exists for.
  test('a preamble temp table applies to a streamed query', async () => {
    const driver = driverWith({
      sqlPreamble: 'CREATE TEMP TABLE preamble_scoped AS SELECT 4 AS v',
    });

    expect(await streamRows(driver, 'SELECT v FROM preamble_scoped')).toEqual([{ v: '4' }]);
  });

  // Replaying the database-scoped part on the second connection raises "already
  // exists"; that must not fail the stream, while the connection-scoped part
  // beside it still has to land.
  test('a mixed database-scoped + connection-scoped preamble streams cleanly', async () => {
    const driver = driverWith({
      sqlPreamble: 'CREATE MACRO mixed() AS 9; CREATE TEMP TABLE mixed_tmp AS SELECT 8 AS v',
    });

    expect(await driver.query('SELECT mixed() AS v', [])).toEqual([{ v: '9' }]);
    expect(await streamRows(driver, 'SELECT mixed() AS m, (SELECT v FROM mixed_tmp) AS t'))
      .toEqual([{ m: '9', t: '8' }]);
  });

  describe('failure posture', () => {
    // A silently skipped preamble meant to define a UDF surfaces later as a
    // baffling "function does not exist", so the new option fails loudly.
    test('sqlPreamble failures surface', async () => {
      const driver = driverWith({ sqlPreamble: 'THIS IS NOT SQL' });

      await expect(driver.query('SELECT 1 AS v', [])).rejects.toThrow();
    });

    // The deprecated name has always swallowed errors and existing deployments
    // may depend on that, so it keeps doing so until the option is removed.
    test('legacy initSql failures are still swallowed', async () => {
      const driver = driverWith({ initSql: 'THIS IS NOT SQL' });

      expect(await driver.query('SELECT 1 AS v', [])).toEqual([{ v: '1' }]);
    });

    test('legacy initSql still applies when it is valid', async () => {
      const driver = driverWith({ initSql: 'CREATE MACRO legacy() AS 5' });

      expect(await driver.query('SELECT legacy() AS v', [])).toEqual([{ v: '5' }]);
    });

    // The stream path opens its own connection and replays the preamble on it.
    // The legacy name has to keep swallowing there too, or a streamed query
    // starts failing on a statement init() would have skipped.
    test('legacy initSql failures are swallowed on the stream path too', async () => {
      const driver = driverWith({ initSql: 'THIS IS NOT SQL' });

      const result = await driver.stream('SELECT 1 AS v', [], { highWaterMark: 100 });

      expect(await streamToArray(result.rowStream as any)).toEqual([{ v: '1' }]);
    });

    // The stream replay must decide "already applied" with the shared
    // `isAlreadyAppliedPreambleError`, not a local regex, so a spelling added to
    // the shared predicate reaches this path too. DuckDB's own duplicate message
    // says "already exists", which a narrower local copy also matched — so the
    // guard is a spelling only the shared predicate knows.
    test('the stream replay defers to the shared already-applied predicate', async () => {
      const driver = driverWith({ sqlPreamble: 'CREATE MACRO shared_pred() AS 4' });

      // Fail the replay with a spelling the shared predicate covers and a
      // local /already exists/i would not.
      const replayed: string[] = [];
      (driver as any).replaySqlPreambleExec = async (sql: string) => {
        replayed.push(sql);
        throw new Error('Catalog Error: Macro Function with name "shared_pred" is already defined!');
      };

      await expect((driver as any).replaySqlPreamble(
        (driver as any).replaySqlPreambleExec,
      )).resolves.toBeUndefined();
      expect(replayed).toEqual(['CREATE MACRO shared_pred() AS 4']);
    });

    test('the stream replay still surfaces a genuine failure', async () => {
      const driver = driverWith({ sqlPreamble: 'CREATE MACRO genuine() AS 1' });

      await expect((driver as any).replaySqlPreamble(async () => {
        throw new Error('Parser Error: syntax error at or near "THIS"');
      })).rejects.toThrow('syntax error');
    });

    test('sqlPreamble takes precedence over legacy initSql', async () => {
      const driver = driverWith({
        sqlPreamble: 'CREATE MACRO pick() AS 2',
        initSql: 'CREATE MACRO pick() AS 1',
      });

      expect(await driver.query('SELECT pick() AS v', [])).toEqual([{ v: '2' }]);
    });
  });
});
