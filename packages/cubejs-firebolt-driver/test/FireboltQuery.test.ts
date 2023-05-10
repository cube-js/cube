import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { FireboltQuery } from '../src/FireboltQuery';

const prepareCompiler = (content: string, options: any[]) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
}, options);

describe('FireboltQuery', () => {
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
    }
  }
});
`,
    []
  );

  it('should use DATE_TRUNC for time granuality dimensions', () => compiler.compile().then(() => {
    const query = new FireboltQuery(
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
      'DATE_TRUNC(\'DAY\', "sales".sales_datetime)'
    );
  }));
});
