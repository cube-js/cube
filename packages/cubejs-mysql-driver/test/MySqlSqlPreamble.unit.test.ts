import { MySqlDriver } from '../src';

// Three separate call sites reach prepareConnection — query, stream and
// downloadQueryResults — and stream bypasses the pool entirely. Each is covered
// here, since wiring only the obvious one would ship a preamble that silently
// vanishes on the others.
const driverWith = (preamble?: string) => {
  const driver = Object.create(MySqlDriver.prototype) as any;
  const executed: string[] = [];

  const conn = {
    execute: async (statement: string) => {
      executed.push(statement);
      return [[], []];
    },
    query: (statement: string, _values: unknown[], cb?: Function) => {
      executed.push(statement);
      if (cb) {
        cb(null, [], []);
        return undefined;
      }
      // The stream path chains .stream() off query().
      return {
        stream: () => {
          const stream: any = {
            on: (event: string, handler: Function) => {
              if (event === 'fields') {
                handler([]);
              }
              return stream;
            },
            pipe: () => stream,
          };
          return stream;
        },
      };
    },
  };

  driver.config = { storeTimezone: '+00:00' };
  driver.sqlPreamble = () => preamble;
  driver.withConnection = async (fn: Function) => fn(conn);
  driver.pool = { _factory: { create: async () => conn, destroy: async () => { /* stubbed */ } } };

  return { driver, executed, conn };
};

const TIME_ZONE = 'SET time_zone = \'+00:00\'';

describe('MySqlDriver sql preamble', () => {
  test('runs the preamble on the query path, after the timezone', async () => {
    const { driver, executed } = driverWith('SET a = 1');

    await driver.query('SELECT 1', []);

    expect(executed).toEqual([TIME_ZONE, 'SET a = 1', 'SELECT 1']);
  });

  test('runs the preamble on the downloadQueryResults path', async () => {
    // This path runs real user SQL and is easy to miss: it neither goes through
    // query() nor through stream().
    const { driver, executed } = driverWith('SET a = 1');

    await driver.downloadQueryResults('SELECT 1', [], {});

    expect(executed).toEqual([TIME_ZONE, 'SET a = 1', 'SELECT 1']);
  });

  test('runs the preamble on the stream path, which bypasses the pool', async () => {
    // stream() takes a connection straight from _factory.create() rather than
    // acquiring from the pool, so it is its own path to the same hook.
    const { driver, executed } = driverWith('SET a = 1');

    await driver.stream('SELECT 1', [], { highWaterMark: 100 });

    expect(executed).toContain('SET a = 1');
    expect(executed.indexOf('SET a = 1')).toBeGreaterThan(executed.indexOf(TIME_ZONE));
  });

  test('runs each statement of a multi-statement preamble', async () => {
    const { driver, executed } = driverWith('SET a = 1; SET b = 2');

    await driver.query('SELECT 1', []);

    expect(executed).toEqual([TIME_ZONE, 'SET a = 1', 'SET b = 2', 'SELECT 1']);
  });

  test('runs nothing extra when no preamble is configured', async () => {
    const { driver, executed } = driverWith(undefined);

    await driver.query('SELECT 1', []);

    expect(executed).toEqual([TIME_ZONE, 'SELECT 1']);
  });

  test('tolerates a preamble statement already applied on a reused connection', async () => {
    const { driver, conn } = driverWith('CREATE TEMPORARY TABLE seen (id int)');
    conn.execute = async (statement: string) => {
      if (statement.startsWith('CREATE')) {
        throw new Error('Table \'seen\' already exists');
      }
      return [[], []];
    };

    await expect(driver.query('SELECT 1', [])).resolves.not.toThrow();
  });

  test('still surfaces a genuine preamble error', async () => {
    const { driver, conn } = driverWith('SET a = 1');
    conn.execute = async (statement: string) => {
      if (statement === 'SET a = 1') {
        throw new Error('Access denied for user');
      }
      return [[], []];
    };

    await expect(driver.query('SELECT 1', [])).rejects.toThrow(/Access denied/);
  });
});
