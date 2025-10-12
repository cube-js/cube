import { OracleQuery } from '../../src/adapter/OracleQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('OracleQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        id: {
          sql: 'id',
          type: 'number',
          primaryKey: true
        },
        createdAt: {
          type: 'time',
          sql: 'created_at'
        }
      }
    })
    `, { adapter: 'oracle' });

  it('uses TO_TIMESTAMP_TZ with milliseconds precision and preserves trailing Z', async () => {
    await compiler.compile();

    const query = new OracleQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['visitors.count'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            dateRange: ['2024-02-01', '2024-02-02'],
            granularity: 'day'
          }
        ],
        timezone: 'UTC'
      }
    );

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).toContain('TO_TIMESTAMP_TZ(:"?", \'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"\')');
    expect(params).toEqual(['2024-02-01T00:00:00.000Z', '2024-02-02T23:59:59.999Z']);
  });
});
