/* eslint-disable no-restricted-syntax */
import { CalciteQuery } from '../../src/adapter/CalciteQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('CalciteQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        },

        unboundedCount: {
          type: 'count',
          rollingWindow: {
            trailing: 'unbounded'
          }
        }
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
        name: {
          type: 'string',
          sql: 'name'
        },
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        }
      }
    })
    `);

  it('uses backtick identifier quoting', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).toContain('`');
    expect(sql).not.toContain('"visitors"');
  });

  it('test equal filters', async () => {
    await compiler.compile();

    const filterValuesVariants: [any[], string][] = [
      [[true], 'WHERE (`visitors`.name = ?)'],
      [[false], 'WHERE (`visitors`.name = ?)'],
      [[''], 'WHERE (`visitors`.name = ?)'],
      [[null], 'WHERE (`visitors`.name IS NULL)'],
    ];

    for (const [values, expected] of filterValuesVariants) {
      const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [],
        filters: [{
          member: 'visitors.name',
          operator: 'equals',
          values
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain(expected);
    }
  });

  it('uses date_trunc for time dimensions', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2020-01-01', '2020-12-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).toMatch(/date_trunc\('day'/i);
  });

  it('uses CAST AS TIMESTAMP for parameterized time stamp casts', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'month',
        dateRange: ['2020-01-01', '2020-12-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).toContain('CAST(? AS TIMESTAMP)');
  });

  it('produces TIMESTAMP literal from quoted string values', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      timeDimensions: [],
      filters: [],
      timezone: 'UTC'
    });

    const result = (query as any).timeStampCast("'2020-01-01T00:00:00.000'");
    expect(result).toBe("TIMESTAMP '2020-01-01 00:00:00'");
  });

  it('does not apply timezone conversion', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2020-01-01', '2020-12-31']
      }],
      timezone: 'America/Los_Angeles'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).not.toContain('AT TIME ZONE');
    expect(sql).not.toContain('CONVERT_TZ');
    expect(sql).not.toContain('@@session.time_zone');
  });

  it('uses LIKE with CONCAT for contains filter (no ILIKE)', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [],
      filters: [{
        member: 'visitors.name',
        operator: 'contains',
        values: ['test']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).not.toContain('ILIKE');
    expect(sql).toContain('LIKE CONCAT');
  });

  it('uses CAST AS VARCHAR for string casting', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'UTC'
    });

    const castResult = (query as any).castToString('my_column');
    expect(castResult).toBe('CAST(my_column AS VARCHAR)');
  });

  it('uses CASE WHEN for segment wrapping', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'UTC'
    });

    const wrappedSql = (query as any).wrapSegmentForDimensionSelect('my_condition');
    expect(wrappedSql).toBe('CASE WHEN my_condition THEN 1 ELSE 0 END');
  });

  it('uses CONCAT for string concatenation', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'UTC'
    });

    const concatResult = (query as any).concatStringsSql(["'a'", "'b'", "'c'"]);
    expect(concatResult).toBe("CONCAT('a', 'b', 'c')");
  });

  it('handles time dimension without granularity in filter', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        dateRange: ['2020-01-01', '2020-12-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).not.toMatch(/GROUP BY.*created_at/i);
    expect(sql).toMatch(/WHERE/i);
  });

  it('handles time dimension with granularity in SELECT and GROUP BY', async () => {
    await compiler.compile();

    const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2020-01-01', '2020-12-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).toMatch(/date_trunc\('day',.*created_at/i);
    expect(sql).toMatch(/GROUP BY/i);
    expect(sql).toMatch(/WHERE/i);
  });

  describe('sqlTemplates', () => {
    it('returns correct template overrides', async () => {
      await compiler.compile();

      const query = new CalciteQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timeDimensions: [],
        filters: [],
        timezone: 'UTC'
      });

      const templates = (query as any).sqlTemplates();

      expect(templates.quotes.identifiers).toBe('`');
      expect(templates.quotes.escape).toBe('\\`');
      expect(templates.types.string).toBe('VARCHAR');
      expect(templates.types.boolean).toBe('BOOLEAN');
      expect(templates.types.timestamp).toBe('TIMESTAMP');
      expect(templates.types.binary).toBe('VARBINARY');
      expect(templates.types.interval).toBeUndefined();
      expect(templates.expressions.ilike).toBeUndefined();
      expect(templates.functions.DATETRUNC).toBe('DATE_TRUNC({{ args_concat }})');
      expect(templates.functions.CURRENTDATE).toBe('CURRENT_DATE');
      expect(templates.functions.PERCENTILECONT).toBeUndefined();
    });
  });
});
