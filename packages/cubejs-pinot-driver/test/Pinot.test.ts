// eslint-disable-next-line import/no-extraneous-dependencies
import { DockerComposeEnvironment, Wait, StartedDockerComposeEnvironment } from 'testcontainers';
import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
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
  });

  // eslint-disable-next-line consistent-return,func-names
  afterAll(async () => {
    if (env) {
      await env.down();
    }
  });

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
  });

  describe('PinotQuery', () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
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
      await compiler.compile();
      const query = new PinotQuery({ joinGraph, cubeEvaluator, compiler }, q);
    
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
    [
      {
        scores__score_date_day: '2024-09-02 00:00:00.0',
        scores__count: 1,
        scores__unbounded_count: 2
      },
      {
        scores__score_date_day: '2024-09-03 00:00:00.0',
        scores__count: 3,
        scores__unbounded_count: 5
      },
      {
        scores__score_date_day: '2024-09-04 00:00:00.0',
        scores__count: 1,
        scores__unbounded_count: 6
      },
    ]));
  });
});
