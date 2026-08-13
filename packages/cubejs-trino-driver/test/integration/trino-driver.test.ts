import { TrinoDriver } from '../../src/TrinoDriver';

const path = require('path');
const { DockerComposeEnvironment, Wait } = require('testcontainers');

describe('TrinoDriver', () => {
  jest.setTimeout(6 * 60 * 1000);

  let env: any;
  let config: any;

  const doWithDriver = async (callback: any) => {
    const driver = new TrinoDriver(config);
    try {
      await callback(driver);
    } finally {
      await driver.release();
    }
  };

  // eslint-disable-next-line consistent-return,func-names
  beforeAll(async () => {
    const authOpts = {
      basic_auth: {
        user: 'presto',
        password: ''
      }
    };

    if (process.env.TEST_PRESTO_HOST) {
      config = {
        host: process.env.TEST_PRESTO_HOST || 'localhost',
        port: process.env.TEST_PRESTO_PORT || '8080',
        catalog: process.env.TEST_PRESTO_CATALOG || 'tpch',
        schema: 'sf1',
        ...authOpts
      };

      return;
    }

    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../../../'),
      'docker-compose.yml'
    );

    env = await dc
      .withStartupTimeout(240 * 1000)
      .withWaitStrategy('coordinator', Wait.forHealthCheck())
      .up();

    config = {
      host: env.getContainer('coordinator').getHost(),
      port: env.getContainer('coordinator').getMappedPort(8080),
      catalog: 'tpch',
      schema: 'sf1',
      ...authOpts
    };
  });

  // eslint-disable-next-line consistent-return,func-names
  afterAll(async () => {
    if (env) {
      await env.down();
    }
  });

  it('should construct', async () => {
    await doWithDriver(() => {
      //
    });
  });

  it('should test connection', async () => {
    await doWithDriver(async (driver: TrinoDriver) => {
      await driver.testConnection();
    });
  });

  it('should test informationSchemaQuery', async () => {
    await doWithDriver(async (driver: any) => {
      const informationSchemaQuery = driver.informationSchemaQuery();
      expect(informationSchemaQuery).toContain('columns.table_schema = \'sf1\'');
    });
  });

  it('should execute a simple SELECT', async () => {
    await doWithDriver(async (driver: TrinoDriver) => {
      const result = await driver.query('SELECT 1 AS num, \'hello\' AS str', []);
      expect(result).toEqual([{ num: 1, str: 'hello' }]);
    });
  });

  it('should handle parameterized queries', async () => {
    await doWithDriver(async (driver: TrinoDriver) => {
      const result = await driver.query('SELECT ? + ? AS sum', [2, 3]);
      expect(result).toEqual([{ sum: 5 }]);
    });
  });

  it('should handle NULL values and empty result sets', async () => {
    await doWithDriver(async (driver: TrinoDriver) => {
      const nulls = await driver.query('SELECT CAST(NULL AS INTEGER) AS empty', []);
      expect(nulls).toEqual([{ empty: null }]);

      const empty = await driver.query('SELECT 1 WHERE 1 = 0', []);
      expect(empty).toEqual([]);
    });
  });

  it('should handle common Trino types', async () => {
    await doWithDriver(async (driver: TrinoDriver) => {
      const result = await driver.query(
        'SELECT CAST(42 AS INTEGER) AS i, CAST(9999999999 AS BIGINT) AS b, true AS flag, DATE \'2024-06-15\' AS d',
        []
      );
      expect(result).toHaveLength(1);
      expect(result[0].i).toBe(42);
      expect(result[0].flag).toBe(true);
      expect(result[0].d).toBeDefined();
    });
  });

  it('should list schemas', async () => {
    await doWithDriver(async (driver: TrinoDriver) => {
      const schemas = await driver.getSchemas();
      expect(schemas.length).toBeGreaterThan(0);
      expect(schemas[0]).toHaveProperty('schema_name');
    });
  });

  it('should stream query results', async () => {
    await doWithDriver(async (driver: TrinoDriver) => {
      const streamResult = await driver.stream(
        'SELECT * FROM (VALUES (1, \'a\'), (2, \'b\'), (3, \'c\')) AS t(id, letter)',
        [],
        { highWaterMark: 1 }
      );

      expect(streamResult.rowStream).toBeDefined();
      expect(streamResult.types!.length).toBeGreaterThan(0);

      const rows: any[] = [];
      await new Promise<void>((resolve, reject) => {
        streamResult.rowStream.on('data', (row: any) => rows.push(row));
        streamResult.rowStream.on('end', resolve);
        streamResult.rowStream.on('error', reject);
      });

      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual({ id: 1, letter: 'a' });
    });
  });

  it('should accumulate multi-batch results in order', async () => {
    await doWithDriver(async (driver: TrinoDriver) => {
      const result = await driver.query(
        'SELECT x FROM UNNEST(sequence(1, 2000)) AS t(x)',
        []
      );
      expect(result).toHaveLength(2000);
      expect(result[0]).toEqual({ x: 1 });
      expect(result[1999]).toEqual({ x: 2000 });
    });
  });

  it('should throw on invalid SQL with a Trino error', async () => {
    await doWithDriver(async (driver: TrinoDriver) => {
      await expect(driver.query('INVALID SQL SYNTAX HERE', [])).rejects.toThrow(/Trino query error/);
    });
  });
});
