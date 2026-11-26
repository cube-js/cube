// eslint-disable-next-line import/no-extraneous-dependencies
import { DockerComposeEnvironment, Wait, StartedDockerComposeEnvironment } from 'testcontainers';
import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { getEnv, streamToArray } from '@cubejs-backend/shared';
import fetch from 'node-fetch';
import type { Readable } from 'stream';
import { PinotQuery } from '../src/PinotQuery';
import { PinotDriver } from '../src/PinotDriver';

const path = require('path');

const prepareCompiler = (content: string, options: any[]) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'postgres', ...options });

describe('Pinot', () => {
  jest.setTimeout(6 * 60 * 1000);

  let env: StartedDockerComposeEnvironment;
  let config: { basicAuth: { user: string, password: string }, host: string, port: string };

  const doWithDriver = async (callback: any) => {
    const driver = new PinotDriver(config);
    const result = await callback(driver);
    return result;
  };

  // The ingestion job returns before the servers have loaded the segment, and a query
  // issued in between answers with no rows instead of failing.
  const waitForRows = async (table: string, expected: number) => {
    const deadline = Date.now() + 60 * 1000;
    let count: unknown;

    while (Date.now() < deadline) {
      try {
        [{ count }] = await doWithDriver((driver: PinotDriver) => driver.query(`SELECT COUNT(*) AS "count" FROM ${table}`, []));
      } catch {
        count = undefined;
      }

      if (Number(count) === expected) {
        return;
      }

      await new Promise((resolve) => { setTimeout(resolve, 500); });
    }

    throw new Error(`Pinot table ${table} has ${count} rows, expected ${expected}`);
  };

  // eslint-disable-next-line consistent-return,func-names
  beforeAll(async () => {
    if (process.env.TEST_PINOT_HOST) {
      config = {
        host: process.env.TEST_PINOT_HOST || 'http://localhost',
        port: process.env.TEST_PINOT_PORT || '8099',
        basicAuth: {
          user: 'admin',
          password: 'mysecret'
        }
      };

      return;
    }

    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../../'),
      'docker-compose.yml'
    );

    env = await dc
      .withStartupTimeout(2 * 60 * 1000)
      .withWaitStrategy('pinot-server-cube-tests', Wait.forHealthCheck())
      .up();

    config = {
      host: `http://${env.getContainer('pinot-broker-cube-tests').getHost()}`,
      port: env.getContainer('pinot-broker-cube-tests').getMappedPort(8099).toString(),
      basicAuth: {
        user: 'admin',
        password: 'mysecret'
      }
    };

    const controller = env.getContainer('pinot-controller-cube-tests');

    await controller.exec(['/opt/pinot/bin/pinot-admin.sh', 'AddTable', '-controllerPort', '9000', '-schemaFile', '/tmp/data/test-resources/students.schema.json', '-tableConfigFile', '/tmp/data/test-resources/students.table.json', '-exec']);
    await controller.exec(['/opt/pinot/bin/pinot-admin.sh', 'AddTable', '-controllerPort', '9000', '-schemaFile', '/tmp/data/test-resources/scores.schema.json', '-tableConfigFile', '/tmp/data/test-resources/scores.table.json', '-exec']);
    await controller.exec(['/opt/pinot/bin/pinot-admin.sh', 'LaunchDataIngestionJob', '-jobSpecFile', '/tmp/data/test-resources/students.jobspec.yml']);
    await controller.exec(['/opt/pinot/bin/pinot-admin.sh', 'LaunchDataIngestionJob', '-jobSpecFile', '/tmp/data/test-resources/scores.jobspec.yml']);

    await waitForRows('students', 3);
    await waitForRows('scores', 6);
  });

  // eslint-disable-next-line consistent-return,func-names
  afterAll(async () => {
    if (env) {
      await env.down();
    }
  });

  const AUTHORIZATION = `Basic ${Buffer.from('admin:mysecret').toString('base64')}`;

  const storedCursors = async (): Promise<string[]> => {
    let failure = '';

    // The broker fails the listing with a 500 when a cursor is deleted while it reads the store.
    for (let attempt = 0; attempt < 5; attempt++) {
      const response = await fetch(`${config.host}:${config.port}/responseStore`, {
        headers: { Authorization: AUTHORIZATION },
      });

      if (response.ok) {
        return (<{ requestId: string }[]> await response.json()).map((cursor) => cursor.requestId);
      }

      failure = `${response.status} ${await response.text()}`;
      await new Promise((resolve) => { setTimeout(resolve, 50); });
    }

    throw new Error(`Unable to list Pinot cursors: ${failure}`);
  };

  const waitForNoStoredCursors = async () => {
    const deadline = Date.now() + 10 * 1000;

    while ((await storedCursors()).length > 0) {
      if (Date.now() > deadline) {
        throw new Error(`Pinot cursors were not deleted: ${await storedCursors()}`);
      }

      await new Promise((resolve) => { setTimeout(resolve, 50); });
    }
  };

  describe('PinotDriver', () => {
    it('constructs', async () => {
      await doWithDriver(() => {
        //
      });
    });
  
    // eslint-disable-next-line func-names
    it('tests the connection', async () => {
      await doWithDriver(async (driver: any) => {
        await driver.testConnection();
      });
    });

    const STUDENT_TYPES = [
      { name: 'studentID', type: 'int' },
      { name: 'firstName', type: 'text' },
    ];

    it('streams query results page by page through a cursor', async () => {
      await doWithDriver(async (driver: PinotDriver) => {
        const source = await driver.stream(
          'SELECT studentID, firstName FROM students WHERE studentID >= ? ORDER BY studentID',
          [200],
          { highWaterMark: 2 }
        );

        try {
          // The result does not fit in the first page, so its cursor has to be alive.
          expect(await storedCursors()).toHaveLength(1);
          expect(source.types).toEqual(STUDENT_TYPES);
          expect(await streamToArray(<any> source.rowStream)).toEqual([
            { studentID: 200, firstName: 'Lucy' },
            { studentID: 201, firstName: 'Bob' },
            { studentID: 202, firstName: 'Nick' },
          ]);
        } finally {
          await source.release?.();
        }

        await waitForNoStoredCursors();
      });
    });

    it('reads the column types of an empty result', async () => {
      await doWithDriver(async (driver: PinotDriver) => {
        const source = await driver.stream(
          'SELECT studentID, firstName FROM students WHERE studentID < ? ORDER BY studentID',
          [0],
          { highWaterMark: 2 }
        );

        try {
          expect(source.types).toEqual(STUDENT_TYPES);
          expect(await streamToArray(<any> source.rowStream)).toEqual([]);
        } finally {
          await source.release?.();
        }

        await waitForNoStoredCursors();
      });
    });

    it('deletes the cursor of a single-page result without a release', async () => {
      await doWithDriver(async (driver: PinotDriver) => {
        const source = await driver.stream(
          'SELECT studentID, firstName FROM students WHERE studentID >= ? ORDER BY studentID',
          [200],
          { highWaterMark: 100 }
        );

        await waitForNoStoredCursors();

        expect(await streamToArray(<any> source.rowStream)).toHaveLength(3);
      });
    });

    it('deletes the cursor when the stream is destroyed', async () => {
      await doWithDriver(async (driver: PinotDriver) => {
        const source = await driver.stream(
          'SELECT studentID, firstName FROM students ORDER BY studentID',
          [],
          { highWaterMark: 1 }
        );
        const rowStream = <Readable> source.rowStream;

        expect(await storedCursors()).toHaveLength(1);

        await new Promise((resolve) => {
          rowStream.once('close', resolve);
          rowStream.destroy();
        });

        await waitForNoStoredCursors();
      });
    });

    it('rejects a stream for a query that fails', async () => {
      await doWithDriver(async (driver: PinotDriver) => {
        await expect(driver.stream('SELECT missingColumn FROM students', [], { highWaterMark: 2 }))
          .rejects.toThrow(/missingColumn/);

        expect(await storedCursors()).toEqual([]);
      });
    });
  });

  describe('PinotQuery', () => {
    const compilers = prepareCompiler(`
      cube(\`students\`, {
        sql_table: 'students',

        dimensions: {
          studentID: {
            type: 'number',
            sql: 'studentID',
            primary_key: true,
            public: true
          },
          firstName: {
            type: 'string',
            sql: 'firstName'
          },
          lastName: {
            type: 'string',
            sql: 'lastName'
          },
          gender: {
            type: 'string',
            sql: 'gender'
          }
        }
      });

      cube(\`scores\`, {
        sql_table: 'scores',

        joins: {
          students: {
            relationship: 'many_to_one',
            sql: \`\${CUBE}.studentID = \${students.studentID}\`,
          },
        },
  
        measures: {
          count: {
            type: 'count',
            sql : '*',
          },
          unboundedCount: {
            type: 'count',
            sql : '*',
            rollingWindow: {
              trailing: 'unbounded'
            }
          },
          maxScore: {
            type: 'max',
            sql: 'score'
          },
          maxScoreEnglish: {
            type: 'max',
            sql: 'score',
            filters: [
              { sql: \`\${CUBE}.subject = 'English'\` }
            ]
          }
        },
  
        dimensions: {
          id: {
            type: 'string',
            sql: \`\${CUBE}.studentID || \${CUBE}.subject || toDateTime(\${CUBE}.score_date, 'yyyy-MM-dd')\`,
            primary_key: true,
            public: true
          },
          scoreDate: {
            type: 'time',
            sql: 'score_date'
          },
          subject: {
            type: 'string',
            sql: 'subject'
          }
        }
      });
      `, []);

    const runQueryTest = async (q: any, expectedResult: any[]) => {
      await compilers.compile();
      const query = compilers.createQuery(PinotQuery, q);
    
      const [sqlQuery, sqlParams] = query.buildSqlAndParams() as [string, unknown[]];

      console.log('SQL To execute', sqlQuery, sqlParams);
    
      const result = await doWithDriver(async (driver: PinotDriver) => driver.query(sqlQuery, sqlParams));
    
      expect(result).toEqual(
        expectedResult
      );
    };

    it('works simple join with equal filters', async () => {
      const filterValuesVariants = [
        [['Lucy'], [{ scores__max_score: 3.8 }]],
        [[null], [{ scores__max_score: null }]],
      ];
    
      for (const [values, expectedResult] of filterValuesVariants) {
        await runQueryTest({
          measures: [
            'scores.maxScore'
          ],
          timeDimensions: [],
          filters: [{
            member: 'students.firstName',
            operator: 'equals',
            values
          }],
          timezone: 'America/Los_Angeles'
        }, expectedResult);
      }
    });

    it('works with a date range', async () => runQueryTest({
      measures: [
        'scores.maxScore'
      ],
      timeDimensions: [
        {
          dimension: 'scores.scoreDate',
          dateRange: ['2024-09-01', '2024-09-07']
        }
      ],
      timezone: 'America/Los_Angeles'
    }, [{ scores__max_score: 3.8 }]));

    it('works with a date range', async () => runQueryTest({
      measures: [
        'scores.maxScore'
      ],
      timeDimensions: [
        {
          dimension: 'scores.scoreDate',
          dateRange: ['2024-09-01', '2024-09-07']
        }
      ],
      timezone: 'America/Los_Angeles'
    }, [{ scores__max_score: 3.8 }]));

    it('works with a filtered measure', async () => runQueryTest({
      measures: [
        'scores.maxScoreEnglish'
      ],
      timeDimensions: [
        {
          dimension: 'scores.scoreDate',
          dateRange: ['2024-09-01', '2024-09-07']
        }
      ],
      timezone: 'America/Los_Angeles'
    }, [{ scores__max_score_english: 3.5 }]));

    it('works with a date range and granularity', async () => runQueryTest({
      measures: [
        'scores.maxScore'
      ],
      timeDimensions: [
        {
          dimension: 'scores.scoreDate',
          dateRange: ['2024-09-01', '2024-09-07'],
          granularity: 'day'
        }
      ],
      timezone: 'America/Los_Angeles',
      order: [
        { id: 'scores.scoreDate' }
      ]
    },
    [
      {
        scores__score_date_day: '2024-09-02 00:00:00.0',
        scores__max_score: 3.2
      },
      {
        scores__score_date_day: '2024-09-03 00:00:00.0',
        scores__max_score: 3.5
      },
      {
        scores__score_date_day: '2024-09-04 00:00:00.0',
        scores__max_score: 3.8
      },
    ]));

    it('groups by the score_date field on the calculated granularity for unbounded trailing windows with dimension', async () => runQueryTest({
      measures: [
        'scores.count', 'scores.unboundedCount'
      ],
      timeDimensions: [
        {
          dimension: 'scores.scoreDate',
          dateRange: ['2024-09-01', '2024-09-07'],
          granularity: 'day'
        }
      ],
      timezone: 'America/Los_Angeles',
      order: [
        { id: 'scores.scoreDate' }
      ]
    },
    // Tesseract returns a row for every day of the date range, the legacy planner
    // only for the days that have scores.
    getEnv('nativeSqlPlanner') ? [
      { scores__score_date_day: '2024-09-01 00:00:00.0', scores__count: null, scores__unbounded_count: 1 },
      { scores__score_date_day: '2024-09-02 00:00:00.0', scores__count: 1, scores__unbounded_count: 2 },
      { scores__score_date_day: '2024-09-03 00:00:00.0', scores__count: 3, scores__unbounded_count: 5 },
      { scores__score_date_day: '2024-09-04 00:00:00.0', scores__count: 1, scores__unbounded_count: 6 },
      { scores__score_date_day: '2024-09-05 00:00:00.0', scores__count: null, scores__unbounded_count: 6 },
      { scores__score_date_day: '2024-09-06 00:00:00.0', scores__count: null, scores__unbounded_count: 6 },
      { scores__score_date_day: '2024-09-07 00:00:00.0', scores__count: null, scores__unbounded_count: 6 },
    ] : [
      { scores__score_date_day: '2024-09-02 00:00:00.0', scores__count: 1, scores__unbounded_count: 2 },
      { scores__score_date_day: '2024-09-03 00:00:00.0', scores__count: 3, scores__unbounded_count: 5 },
      { scores__score_date_day: '2024-09-04 00:00:00.0', scores__count: 1, scores__unbounded_count: 6 },
    ]));
  });
});
