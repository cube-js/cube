// eslint-disable-next-line import/no-extraneous-dependencies
import { DockerComposeEnvironment, StartedDockerComposeEnvironment, Wait } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import path from 'path';

import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

import { DruidDriver, DruidDriverConfiguration } from '../src/DruidDriver';
import { DruidQuery } from '../src/DruidQuery';

// A LIKE filter has to match the user's value literally, and that only works if
// the escaping applied to the value reaches Druid with the clause that
// interprets it. Druid accepts a non-literal pattern and honours ESCAPE on it
// only over a real datasource, so this needs one rather than an inline SELECT.
const LIKE_DATASOURCE = 'like_escape_filters';
const LIKE_ROWS = ['50%Yoff', '50%_off', '50Xyoff', 'off', 'plain', 'a\\b', 'aXb'];

const LIKE_CASES: [string, string, string[]][] = [
  ['contains', '%', ['50%Yoff', '50%_off']],
  ['contains', '_', ['50%_off']],
  ['notContains', '%', ['50Xyoff', 'off', 'plain', 'a\\b', 'aXb']],
  ['startsWith', '50%', ['50%Yoff', '50%_off']],
  ['endsWith', '_off', ['50%_off']],
  // The escape character is the third thing escaped in a value, and getting it
  // wrong costs a row rather than adding one - so `aXb` stands by as the decoy.
  ['contains', 'a\\b', ['a\\b']],
  // An ordinary value has to keep working: escaping must not break plain search.
  ['contains', 'off', ['50%Yoff', '50%_off', '50Xyoff', 'off']],
];

const LIKE_MODEL = `
  cube('names', {
    sql: \`SELECT * FROM ${LIKE_DATASOURCE}\`,
    measures: { count: { type: 'count' } },
    dimensions: { name: { sql: 'name', type: 'string' } },
  });
`;

describe('DruidDriver', () => {
  let env: StartedDockerComposeEnvironment | null = null;
  let config: DruidDriverConfiguration;

  const doWithDriver = async (callback: (driver: DruidDriver) => Promise<any>) => {
    const driver = new DruidDriver(config);

    await callback(driver);
  };

  // eslint-disable-next-line consistent-return
  beforeAll(async () => {
    if (process.env.TEST_DRUID_HOST) {
      const host = process.env.TEST_DRUID_HOST || 'localhost';
      const port = process.env.TEST_DRUID_PORT || '8888';

      config = {
        url: `http://${host}:${port}`,
        user: 'admin',
        password: 'password1',
      };

      return;
    }

    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../../'),
      'docker-compose.yml'
    );

    env = await dc
      .withWaitStrategy('zookeeper', Wait.forLogMessage('binding to port /0.0.0.0:2181'))
      .withWaitStrategy('postgres', Wait.forHealthCheck())
      .withWaitStrategy('router', Wait.forHealthCheck())
      .withWaitStrategy('middlemanager', Wait.forHealthCheck())
      .withWaitStrategy('historical', Wait.forHealthCheck())
      .withWaitStrategy('broker', Wait.forHealthCheck())
      .withWaitStrategy('coordinator', Wait.forHealthCheck())
      .up();

    const host = env.getContainer('router').getHost();
    const port = env.getContainer('router').getMappedPort(8888);

    config = {
      user: 'admin',
      password: 'password1',
      url: `http://${host}:${port}`,
    };
  }, 2 * 60 * 1000);

  // eslint-disable-next-line consistent-return
  afterAll(async () => {
    if (env) {
      await env.down();
    }
  }, 30 * 1000);

  it('should construct', async () => {
    jest.setTimeout(10 * 1000);

    return doWithDriver(async () => {
      //
    });
  });

  it('should test connection', async () => {
    jest.setTimeout(10 * 1000);

    return doWithDriver(async (driver) => {
      await driver.testConnection();
    });
  });

  it('SELECT 1', async () => {
    jest.setTimeout(10 * 1000);

    return doWithDriver(async (driver) => {
      expect(await driver.query('SELECT 1')).toEqual([{
        EXPR$0: 1,
      }]);
    });
  });

  it('downloadQueryResults', async () => {
    jest.setTimeout(10 * 1000);

    return doWithDriver(async (driver) => {
      const result = await driver.downloadQueryResults(
        'SELECT 1 as id, true as finished, \'netherlands\' as country, CAST(\'2020-01-01T01:01:01.111Z\' as timestamp) as created UNION ALL SELECT 2 as id, false as finished, \'spain\' as country, CAST(\'2020-01-01T01:01:01.111Z\' as timestamp) as created',
        [],
        { highWaterMark: 1 }
      );
      expect(result).toEqual({
        rows: [
          { country: 'netherlands', created: '2020-01-01T01:01:01.111Z', finished: true, id: 1 },
          { country: 'spain', created: '2020-01-01T01:01:01.111Z', finished: false, id: 2 }
        ],
        types: [
          { name: 'id', type: 'int' },
          { name: 'finished', type: 'boolean' },
          { name: 'country', type: 'text' },
          { name: 'created', type: 'timestamp' }
        ]
      });
    });
  });

  const druidPost = async (endpoint: string, payload: unknown) => {
    const response = await fetch(`${config.url}${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Basic ${Buffer.from(`${config.user}:${config.password}`).toString('base64')}`,
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      throw new Error(`${endpoint} responded ${response.status}: ${await response.text()}`);
    }

    return response.json();
  };

  const ingestLikeRows = async () => {
    const { task } = await druidPost('/druid/indexer/v1/task', {
      type: 'index_parallel',
      spec: {
        ioConfig: {
          type: 'index_parallel',
          inputSource: {
            type: 'inline',
            data: LIKE_ROWS.map(name => JSON.stringify({ ts: '2020-01-01T00:00:00Z', name })).join('\n'),
          },
          inputFormat: { type: 'json' },
        },
        dataSchema: {
          dataSource: LIKE_DATASOURCE,
          timestampSpec: { column: 'ts', format: 'iso' },
          dimensionsSpec: { dimensions: ['name'] },
          granularitySpec: { queryGranularity: 'none', rollup: false, segmentGranularity: 'day' },
        },
        tuningConfig: { type: 'index_parallel' },
      },
    }) as { task: string };

    // Ingestion finishing and the segment becoming queryable are separate
    // events, so wait for the rows themselves rather than for the task.
    const deadline = Date.now() + 4 * 60 * 1000;

    while (Date.now() < deadline) {
      const driver = new DruidDriver(config);

      try {
        const rows = await driver.query<Record<string, unknown>>(`SELECT COUNT(*) AS c FROM ${LIKE_DATASOURCE}`, []);

        if (Number(Object.values(rows[0])[0]) === LIKE_ROWS.length) {
          return;
        }
      } catch {
        // the datasource is not there yet
      } finally {
        await driver.release();
      }

      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    throw new Error(`Ingestion task ${task} did not make ${LIKE_DATASOURCE} queryable in time`);
  };

  const filteredNames = async (operator: string, value: string, useNativeSqlPlanner: boolean) => {
    const { compiler, joinGraph, cubeEvaluator } = originalPrepareCompiler({
      localPath: () => __dirname,
      dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content: LIKE_MODEL }]),
    }, { adapter: 'druid' });

    await compiler.compile();

    const query = new DruidQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['names.name'],
      filters: [{ member: 'names.name', operator, values: [value] }],
      useNativeSqlPlanner,
    });

    const [sql, params] = query.buildSqlAndParams();
    const driver = new DruidDriver(config);

    try {
      const rows = await driver.query<Record<string, unknown>>(sql, params);

      return rows.map(row => Object.values(row)[0]).sort();
    } finally {
      await driver.release();
    }
  };

  describe('LIKE filters match the value literally', () => {
    beforeAll(async () => {
      await ingestLikeRows();
    }, 5 * 60 * 1000);

    it.each(LIKE_CASES)('%s %p on the legacy planner', async (operator, value, expected) => {
      expect(await filteredNames(operator, value, false)).toEqual([...expected].sort());
    }, 60 * 1000);

    it.each(LIKE_CASES)('%s %p on the tesseract planner', async (operator, value, expected) => {
      expect(await filteredNames(operator, value, true)).toEqual([...expected].sort());
    }, 60 * 1000);
  });
});
