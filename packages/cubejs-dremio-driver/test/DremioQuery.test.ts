import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

const DremioQuery = require('../../driver/DremioQuery');

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
});

describe('DremioQuery', () => {

  jest.setTimeout(10 * 60 * 1000); // Engine needs to spin up

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(
    `
cube(\`sales\`, {
  sql: \` select * from public.sales \`,

  measures: {
    count: {
      type: 'count'
    }
  },
  dimensions: {
    category: {
      type: 'string',
      sql: 'category'
    },
    salesDatetime: {
      type: 'time',
      sql: 'sales_datetime'
    },
    isShiped: {
      type: 'boolean',
      sql: 'is_shiped',
    },
  }
});
`,
  );

  it('should use DATE_TRUNC for time granularity dimensions', () => compiler.compile().then(() => {
    const query = new DremioQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['sales.count'],
        timeDimensions: [
          {
            dimension: 'sales.salesDatetime',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-02'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'sales.salesDatetime',
          },
        ],
      }
    );

    const queryAndParams = query.buildSqlAndParams();

    expect(queryAndParams[0]).toContain(
      'DATE_TRUNC(\'day\', CONVERT_TIMEZONE(\'America/Los_Angeles\', "sales".sales_datetime))'
    );
  }));

  it('should cast BOOLEAN', () => compiler.compile().then(() => {
    const query = new DremioQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['sales.count'],
        filters: [
          {
            member: 'sales.isShiped',
            operator: 'equals',
            values: ['true']
          }
        ]
      }
    );

    const queryAndParams = query.buildSqlAndParams();

    expect(queryAndParams[0]).toContain(
      '("sales".is_shiped = CAST(? AS BOOLEAN))'
    );

    expect(queryAndParams[1]).toEqual(['true']);
  }));

  it('should cast timestamp', () => compiler.compile().then(() => {
    const query = new DremioQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['sales.count'],
        timeDimensions: [
          {
            dimension: 'sales.salesDatetime',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-02'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'sales.salesDatetime',
          },
        ],
      }
    );

    const queryAndParams = query.buildSqlAndParams();

    expect(queryAndParams[0]).toContain(
      '("sales".sales_datetime >= TO_TIMESTAMP(?, \'YYYY-MM-DD"T"HH24:MI:SS.FFF\') AND "sales".sales_datetime <= TO_TIMESTAMP(?, \'YYYY-MM-DD"T"HH24:MI:SS.FFF\'))'
    );
  }));
});
