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

  it('generates TO_TIMESTAMP_TZ with millisecond precision for date range filters', async () => {
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

    // Verify TO_TIMESTAMP_TZ is used with proper ISO 8601 format including milliseconds
    expect(sql).toContain('TO_TIMESTAMP_TZ(:"?", \'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"\')');
    expect(sql).toMatch(/created_at\s+>=\s+TO_TIMESTAMP_TZ/);
    expect(sql).toMatch(/created_at\s+<=\s+TO_TIMESTAMP_TZ/);
    
    // Verify parameters include millisecond precision
    expect(params).toEqual(['2024-02-01T00:00:00.000Z', '2024-02-02T23:59:59.999Z']);
  });

  it('generates TRUNC function for day granularity grouping', async () => {
    await compiler.compile();

    const query = new OracleQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['visitors.count'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            dateRange: ['2024-01-01', '2024-01-31'],
            granularity: 'day'
          }
        ],
        timezone: 'UTC'
      }
    );

    const [sql, params] = query.buildSqlAndParams();

    // Verify TRUNC with DD format for day grouping
    expect(sql).toContain('TRUNC("visitors".created_at, \'DD\')');
    expect(sql).toMatch(/GROUP BY\s+TRUNC/);
    expect(params).toEqual(['2024-01-01T00:00:00.000Z', '2024-01-31T23:59:59.999Z']);
  });

  it('generates TRUNC function for month granularity grouping', async () => {
    await compiler.compile();

    const query = new OracleQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['visitors.count'],
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            dateRange: ['2024-01-01', '2024-12-31'],
            granularity: 'month'
          }
        ],
        timezone: 'UTC'
      }
    );

    const [sql, params] = query.buildSqlAndParams();

    // Verify TRUNC with MM format for month grouping
    expect(sql).toContain('TRUNC("visitors".created_at, \'MM\')');
    expect(sql).toMatch(/GROUP BY\s+TRUNC/);
    expect(params).toEqual(['2024-01-01T00:00:00.000Z', '2024-12-31T23:59:59.999Z']);
  });
});
