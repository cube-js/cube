// Keep the JVM out of the unit test: preamble resolution is pure logic.
jest.mock('@cubejs-backend/jdbc/lib/drivermanager', () => ({
  getConnection: () => { /* stub */ },
}), { virtual: true });
jest.mock('@cubejs-backend/jdbc/lib/connection', () => class Connection {
  public getMetaData() { /* stub */ }
}, { virtual: true });
jest.mock('@cubejs-backend/jdbc/lib/databasemetadata', () => class DatabaseMetaData {
  public getSchemas() { /* stub */ }

  public getTables() { /* stub */ }
}, { virtual: true });
jest.mock('@cubejs-backend/jdbc/lib/jinst', () => ({ isJvmCreated: () => true }), { virtual: true });
jest.mock('@cubejs-backend/node-java-maven', () => () => { /* stub */ }, { virtual: true });

// eslint-disable-next-line import/first
import { JDBCDriver } from '../../src/JDBCDriver';

const MYSQL_BUILT_IN = 'SET time_zone = \'+00:00\'';

// The constructor requires a JVM-backed pool, while preamble resolution only
// depends on the config.
const driverFor = (config: Record<string, any>): any => {
  const driver = Object.create(JDBCDriver.prototype);
  driver.config = config;
  driver.logger = jest.fn();
  return driver;
};

describe('JDBC sql preamble', () => {
  afterEach(() => {
    delete process.env.CUBEJS_DB_SQL_PREAMBLE;
  });

  // Read by the unapplied-preamble warning at driver resolution. Declared here
  // rather than in a list of dbTypes so every JDBC-based driver — Databricks
  // among them — inherits the right answer without being enumerated.
  it('declares that it applies the preamble', () => {
    expect(driverFor({ dbType: 'athena' }).supportsSqlPreamble()).toBe(true);
  });

  it('keeps the per-dbType built-ins when nothing is configured', () => {
    expect(driverFor({ dbType: 'mysql' }).prepareConnectionQueries()).toEqual([MYSQL_BUILT_IN]);
    expect(driverFor({ dbType: 'athena' }).prepareConnectionQueries()).toEqual([]);
  });

  // Decision 8: replacing the built-in would silently drop the timezone
  // guarantee MySQL depends on to read timestamps.
  it('appends sqlPreamble after the built-ins', () => {
    expect(driverFor({ dbType: 'mysql', sqlPreamble: 'SET a = 1' }).prepareConnectionQueries())
      .toEqual([MYSQL_BUILT_IN, 'SET a = 1']);
  });

  it('splits a multi-statement sqlPreamble into separate statements', () => {
    expect(driverFor({ dbType: 'athena', sqlPreamble: 'SET a = 1; SET b = 2' }).prepareConnectionQueries())
      .toEqual(['SET a = 1', 'SET b = 2']);
  });

  it('does not split a semicolon inside a UDF body', () => {
    const preamble = 'CREATE FUNCTION f() AS $$ SELECT 1; $$; SET a = 1';

    expect(driverFor({ dbType: 'athena', sqlPreamble: preamble }).prepareConnectionQueries())
      .toEqual(['CREATE FUNCTION f() AS $$ SELECT 1; $$', 'SET a = 1']);
  });

  it('reads the env var when no config value is set', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET from_env = 1';

    expect(driverFor({ dbType: 'mysql' }).prepareConnectionQueries())
      .toEqual([MYSQL_BUILT_IN, 'SET from_env = 1']);
  });

  it('prefers the config value over the env var', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET from_env = 1';

    expect(driverFor({ dbType: 'athena', sqlPreamble: 'SET from_config = 1' }).prepareConnectionQueries())
      .toEqual(['SET from_config = 1']);
  });

  it('treats a blank preamble as not configured', () => {
    expect(driverFor({ dbType: 'mysql', sqlPreamble: '   ' }).prepareConnectionQueries())
      .toEqual([MYSQL_BUILT_IN]);
  });

  describe('deprecated prepareConnectionQueries', () => {
    // This option has always REPLACED the built-ins, so someone who set it to
    // override the MySQL timezone still gets only their own statements. Making
    // it append would change behavior for existing deployments.
    it('still replaces the built-ins rather than appending', () => {
      const driver = driverFor({ dbType: 'mysql', prepareConnectionQueries: ['SET time_zone = \'+03:00\''] });

      expect(driver.prepareConnectionQueries()).toEqual(['SET time_zone = \'+03:00\'']);
      expect(driver.prepareConnectionQueries()).not.toContain(MYSQL_BUILT_IN);
    });

    it('warns that the option is deprecated', () => {
      const driver = driverFor({ dbType: 'mysql', prepareConnectionQueries: ['SET a = 1'] });

      driver.prepareConnectionQueries();

      expect(driver.logger).toHaveBeenCalledTimes(1);
      expect(driver.logger.mock.calls[0][1].warning).toContain('prepareConnectionQueries');
      expect(driver.logger.mock.calls[0][1].warning).toContain('sqlPreamble');
    });

    // This runs per query, so an unlatched warning would emit thousands of
    // identical log lines a minute under load.
    it('warns once per driver instance, not once per query', () => {
      const driver = driverFor({ dbType: 'mysql', prepareConnectionQueries: ['SET a = 1'] });

      driver.prepareConnectionQueries();
      driver.prepareConnectionQueries();
      driver.prepareConnectionQueries();

      expect(driver.logger).toHaveBeenCalledTimes(1);
    });

    it('is overridden by the env var, which is not silently dropped', () => {
      process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET from_env = 1';
      const driver = driverFor({ dbType: 'athena', prepareConnectionQueries: ['SET old = 1'] });

      expect(driver.prepareConnectionQueries()).toEqual(['SET from_env = 1']);
    });

    it('is kept when sqlPreamble is blank, rather than silently discarded', () => {
      const driver = driverFor({
        dbType: 'mysql',
        sqlPreamble: '   ',
        prepareConnectionQueries: ['SET time_zone = \'+03:00\''],
      });

      expect(driver.prepareConnectionQueries()).toEqual(['SET time_zone = \'+03:00\'']);
    });

    it('is overridden by sqlPreamble, which appends as usual', () => {
      const driver = driverFor({
        dbType: 'mysql',
        sqlPreamble: 'SET new = 1',
        prepareConnectionQueries: ['SET old = 1'],
      });

      expect(driver.prepareConnectionQueries()).toEqual([MYSQL_BUILT_IN, 'SET new = 1']);
      expect(driver.logger).not.toHaveBeenCalled();
    });

    it('falls back to the built-ins when set to an empty array', () => {
      expect(driverFor({ dbType: 'mysql', prepareConnectionQueries: [] }).prepareConnectionQueries())
        .toEqual([MYSQL_BUILT_IN]);
    });
  });

  // Asserting the resolver's return value is not enough: the statements have to
  // reach the connection. Both paths ran the loop against the wrong shape (or
  // not at all) while a return-value test stayed green, so these assert the
  // execution, statement by statement, in order.
  describe('execution on the connection', () => {
    const executingDriverFor = (config: Record<string, any>) => {
      const driver = driverFor(config);
      const conn = {
        createStatement: (cb: Function) => cb(null, {
          cancel: (cb2: Function) => cb2(null),
          execute: (_sql: string, cb2: Function) => cb2(null, {
            toObjectIter: (cb3: Function) => cb3(null, {
              labels: [],
              types: [],
              rows: { next: () => ({ done: true }) },
            }),
          }),
        }),
      };

      driver.pool = {
        acquire: async () => conn,
        release: async () => { /* nothing to release in the harness */ },
      };
      driver.executed = [];
      driver.executeStatement = jest.fn(async (_conn: unknown, sql: string) => {
        driver.executed.push(sql);
        return [];
      });

      return driver;
    };

    it('runs the built-in and the preamble on the query path, built-in first', async () => {
      const driver = executingDriverFor({ dbType: 'mysql', sqlPreamble: 'SET a = 1' });

      await driver.query('SELECT 1', []);

      expect(driver.executed).toEqual([MYSQL_BUILT_IN, 'SET a = 1', 'SELECT 1']);
    });

    it('runs each statement of a multi-statement preamble on the query path', async () => {
      const driver = executingDriverFor({ dbType: 'athena', sqlPreamble: 'SET a = 1; SET b = 2' });

      await driver.query('SELECT 1', []);

      expect(driver.executed).toEqual(['SET a = 1', 'SET b = 2', 'SELECT 1']);
    });

    it('runs the preamble on the stream path too', async () => {
      const driver = executingDriverFor({ dbType: 'mysql', sqlPreamble: 'SET a = 1' });

      await driver.stream('SELECT 1', [], { highWaterMark: 100 });

      // The streamed query itself goes through createStatement rather than
      // executeStatement, so only the preamble shows up here.
      expect(driver.executed).toEqual([MYSQL_BUILT_IN, 'SET a = 1']);
    });

    it('runs nothing extra on either path when no preamble is configured', async () => {
      const queryDriver = executingDriverFor({ dbType: 'athena' });
      await queryDriver.query('SELECT 1', []);
      expect(queryDriver.executed).toEqual(['SELECT 1']);

      const streamDriver = executingDriverFor({ dbType: 'athena' });
      await streamDriver.stream('SELECT 1', [], { highWaterMark: 100 });
      expect(streamDriver.executed).toEqual([]);
    });
  });

  // This driver pools connections, so the preamble is replayed on every acquire.
  // A `CREATE …` — the feature's headline use case — therefore hits "already
  // exists" as soon as a connection is reused, and that has to be tolerated the
  // way the Postgres and MySQL drivers tolerate it.
  describe('a reused pooled connection', () => {
    const CREATE = 'CREATE FUNCTION median(x INT) RETURNS INT RETURN x';

    // Fails the CREATE the way an engine does on a connection that already ran
    // it, and records everything attempted.
    const reusedConnectionDriverFor = (config: Record<string, any>) => {
      const driver = driverFor(config);
      const conn = {
        createStatement: (cb: Function) => cb(null, {
          cancel: (cb2: Function) => cb2(null),
          execute: (_sql: string, cb2: Function) => cb2(null, {
            toObjectIter: (cb3: Function) => cb3(null, {
              labels: [],
              types: [],
              rows: { next: () => ({ done: true }) },
            }),
          }),
        }),
      };

      driver.pool = {
        acquire: async () => conn,
        release: async () => { /* nothing to release in the harness */ },
      };
      driver.executed = [];
      driver.executeStatement = jest.fn(async (_conn: unknown, sql: string) => {
        driver.executed.push(sql);

        if (sql === CREATE) {
          throw new Error('function "median" already exists with same argument types');
        }

        return [];
      });

      return driver;
    };

    it('tolerates an already-applied preamble statement on the query path', async () => {
      const driver = reusedConnectionDriverFor({ dbType: 'athena', sqlPreamble: `${CREATE}; SET a = 1` });

      await expect(driver.query('SELECT median(1)', [])).resolves.toEqual([]);
      // The later statements still run, and so does the primary query.
      expect(driver.executed).toEqual([CREATE, 'SET a = 1', 'SELECT median(1)']);
    });

    it('tolerates an already-applied preamble statement on the stream path', async () => {
      const driver = reusedConnectionDriverFor({ dbType: 'athena', sqlPreamble: `${CREATE}; SET a = 1` });

      await expect(driver.stream('SELECT median(1)', [], { highWaterMark: 100 })).resolves.toBeDefined();
      expect(driver.executed).toEqual([CREATE, 'SET a = 1']);
    });

    it('still surfaces a genuine preamble failure', async () => {
      const driver = driverFor({ dbType: 'athena', sqlPreamble: 'THIS IS NOT SQL' });
      driver.pool = {
        acquire: async () => ({}),
        release: async () => { /* nothing to release in the harness */ },
      };
      driver.executeStatement = jest.fn(async () => {
        throw new Error('syntax error at or near "THIS"');
      });

      await expect(driver.query('SELECT 1', [])).rejects.toThrow('syntax error');
    });

    // The built-ins are idempotent `SET`s, and the deprecated option's contents
    // are the user's own with semantics that must not change — so a failure in
    // either still surfaces rather than being swallowed as "already applied".
    it('does not extend the tolerance to the built-in connection queries', async () => {
      const driver = driverFor({ dbType: 'mysql', sqlPreamble: 'SET a = 1' });
      driver.pool = {
        acquire: async () => ({}),
        release: async () => { /* nothing to release in the harness */ },
      };
      driver.executeStatement = jest.fn(async (_conn: unknown, sql: string) => {
        if (sql === MYSQL_BUILT_IN) {
          throw new Error('time_zone already exists');
        }
        return [];
      });

      await expect(driver.query('SELECT 1', [])).rejects.toThrow('already exists');
    });

    // The split is positional (the trailing N entries), so it stays correct even
    // when a preamble statement is textually identical to a built-in: both still
    // run, in order, and only the preamble copy gets the tolerance.
    it('splits by position when the preamble repeats a built-in verbatim', async () => {
      const driver = reusedConnectionDriverFor({ dbType: 'mysql', sqlPreamble: MYSQL_BUILT_IN });

      await expect(driver.query('SELECT 1', [])).resolves.toEqual([]);
      expect(driver.executed).toEqual([MYSQL_BUILT_IN, MYSQL_BUILT_IN, 'SELECT 1']);
    });

    it('does not extend the tolerance to the deprecated option', async () => {
      const driver = driverFor({ dbType: 'athena', prepareConnectionQueries: ['SET legacy = 1'] });
      driver.pool = {
        acquire: async () => ({}),
        release: async () => { /* nothing to release in the harness */ },
      };
      driver.executeStatement = jest.fn(async (_conn: unknown, sql: string) => {
        if (sql === 'SET legacy = 1') {
          throw new Error('legacy already exists');
        }
        return [];
      });

      await expect(driver.query('SELECT 1', [])).rejects.toThrow('already exists');
    });
  });
});
