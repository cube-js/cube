import { QueryAlias } from '@cubejs-backend/shared';
import { MssqlQuery } from '../../src/adapter/MssqlQuery';
import { prepareJsCompiler } from './PrepareCompiler';
import { createJoinedCubesSchema } from './utils';

describe('MssqlQuery', () => {
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
        id: {
          sql: 'id',
          type: 'number',
          primaryKey: true,
        },

        createdAt: {
          type: 'time',
          sql: 'created_at'
        },

        source: {
          type: 'string',
          sql: 'source'
        },
      }
    })

    cube(\`Deals\`, {
      sql: \`select * from deals\`,

      measures: {
        amount: {
          sql: \`amount\`,
          type: \`sum\`
        }
      },

      dimensions: {
        salesManagerId: {
          sql: \`sales_manager_id\`,
          type: 'string',
          primaryKey: true
        }
      }
    })

    cube(\`SalesManagers\`, {
      sql: \`select * from sales_managers\`,

      joins: {
        Deals: {
          relationship: \`hasMany\`,
          sql: \`\${SalesManagers}.id = \${Deals}.sales_manager_id\`
        }
      },

      measures: {
        averageDealAmount: {
          sql: \`\${dealsAmount}\`,
          type: \`avg\`
        }
      },

      dimensions: {
        id: {
          sql: \`id\`,
          type: \`string\`,
          primaryKey: true
        },

        dealsAmount: {
          sql: \`\${Deals.amount}\`,
          type: \`number\`,
          subQuery: true
        }
      }
    });
    `);

  const joinedSchemaCompilers = prepareJsCompiler(createJoinedCubesSchema());

  it('renders SQL API pushdown joins after FROM and before WHERE', async () => {
    await compiler.compile();

    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
    });

    // The SQL API supplies already-rendered joins to statements.select.
    // Omitting this loop leaves projected columns referring to absent aliases.
    const { select } = query.sqlTemplates().statements;
    const joins = '{% for join in joins %}\n{{ join }}{% endfor %}';
    expect(select).toContain(joins);
    expect(select.indexOf(joins)).toBeGreaterThan(select.indexOf('FROM {{ from_prepared }}'));
    expect(select.indexOf(joins)).toBeGreaterThan(select.indexOf(') AS {{ from_alias }}'));
    expect(select.indexOf(joins)).toBeLessThan(select.indexOf('{% if filter %}'));
  });

  it('should group by the created_at field on the calculated granularity for unbounded trailing windows',
    () => compiler.compile().then(() => {
      const query = new MssqlQuery(
        { joinGraph, cubeEvaluator, compiler },
        {
          measures: ['visitors.count', 'visitors.unboundedCount'],
          timeDimensions: [
            {
              dimension: 'visitors.createdAt',
              granularity: 'week',
              dateRange: ['2017-01-01', '2017-01-30'],
            },
          ],
          timezone: 'America/Los_Angeles',
          order: [
            {
              id: 'visitors.createdAt',
            },
          ],
        }
      );

      const queryAndParams = query.buildSqlAndParams();

      const queryString = queryAndParams[0];
      // The native planner groups by the calculated-granularity expression
      // directly (the legacy planner grouped by a time-series CTE alias).
      expect(queryString).toContain('GROUP BY dateadd(week, DATEDIFF(week, 0, CAST("visitors".created_at AT TIME ZONE \'UTC\' AT TIME ZONE \'Pacific Standard Time\' AS DATETIME2)), 0)');
    }));

  it('should group by both time and regular dimensions on rolling windows',
    () => compiler.compile().then(() => {
      const query = new MssqlQuery(
        { joinGraph, cubeEvaluator, compiler },
        {
          measures: ['visitors.count', 'visitors.unboundedCount'],
          dimensions: ['visitors.source'],
          timeDimensions: [
            {
              dimension: 'visitors.createdAt',
              granularity: 'week',
              dateRange: ['2017-01-01', '2017-01-30'],
            },
          ],
          timezone: 'America/Los_Angeles',
          order: [
            {
              id: 'visitors.createdAt',
            },
          ],
        }
      );

      const queryAndParams = query.buildSqlAndParams();

      const queryString = queryAndParams[0];
      // The native planner groups by the regular dimension and the
      // calculated-granularity expression (legacy used a time-series CTE alias).
      expect(queryString).toContain('GROUP BY "visitors".source, dateadd(week, DATEDIFF(week, 0, CAST("visitors".created_at AT TIME ZONE \'UTC\' AT TIME ZONE \'Pacific Standard Time\' AS DATETIME2)), 0)');
    }));

  it('should not include order by clauses in subqueries',
    () => compiler.compile().then(() => {
      const query = new MssqlQuery(
        { joinGraph, cubeEvaluator, compiler },
        {
          dimensions: ['SalesManagers.id', 'SalesManagers.dealsAmount'],
        }
      );

      const subQueryDimensions = query.collectFromMembers(
        false,
        query.collectSubQueryDimensionsFor.bind(query),
        'collectSubQueryDimensionsFor'
      );

      const queryAndParams = query.buildSqlAndParams();
      const subQuery: any = query.subQueryJoin(subQueryDimensions[0]);

      expect(/ORDER BY/.test(subQuery.sql)).toEqual(false);
      expect(queryAndParams[0]).toMatch(/ORDER BY/);
    }));

  it('should not include group by clauses if ungrouped is set to true in query',
    () => compiler.compile().then(() => {
      const query = new MssqlQuery(
        { joinGraph, cubeEvaluator, compiler },
        {
          dimensions: ['visitors.createdAt', 'visitors.source'],
          ungrouped: true,
          allowUngroupedWithoutPrimaryKey: true,
        }
      );

      const queryAndParams = query.buildSqlAndParams();
      const queryString = queryAndParams[0];

      expect(/GROUP BY/.test(queryString)).toEqual(false);
    }));

  it('renders rowLimit: 0 as TOP 0 without an invalid FETCH NEXT 0', async () => {
    await compiler.compile();

    // With an ORDER BY the template would normally emit OFFSET/FETCH NEXT, but T-SQL
    // rejects `FETCH NEXT 0 ROWS ONLY`, so a zero limit has to go through TOP
    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      dimensions: ['visitors.source'],
      order: [{ id: 'visitors.source', desc: false }],
      timezone: 'UTC',
      rowLimit: 0,
    });

    const sql = query.buildSqlAndParams()[0];

    expect(sql).toContain('TOP 0');
    expect(sql).not.toContain('FETCH NEXT');
  });

  it('renders rowLimit: 0 with an offset as TOP 0 and no OFFSET tail', async () => {
    await compiler.compile();

    // T-SQL forbids TOP together with OFFSET/FETCH, and a zero limit yields no rows
    // whatever the offset is, so the whole OFFSET/FETCH tail has to go
    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      dimensions: ['visitors.source'],
      order: [{ id: 'visitors.source', desc: false }],
      timezone: 'UTC',
      rowLimit: 0,
      offset: 10,
    });

    const sql = query.buildSqlAndParams()[0];

    expect(sql).toContain('TOP 0');
    expect(sql).not.toContain('FETCH NEXT');
    expect(sql).not.toContain('OFFSET');
  });

  it('still renders OFFSET/FETCH NEXT for a non-zero rowLimit with an offset', async () => {
    await compiler.compile();

    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      dimensions: ['visitors.source'],
      order: [{ id: 'visitors.source', desc: false }],
      timezone: 'UTC',
      rowLimit: 5,
      offset: 10,
    });

    const sql = query.buildSqlAndParams()[0];

    expect(sql).toContain('OFFSET 10 ROWS');
    expect(sql).toContain('FETCH NEXT 5 ROWS ONLY');
    expect(sql).not.toContain('TOP');
  });

  it('renders DISTINCT before TOP in the select template', async () => {
    await compiler.compile();

    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      timezone: 'UTC',
      rowLimit: 0,
    });

    // T-SQL clause order is SELECT [ALL | DISTINCT] [TOP (expr)], so `SELECT TOP 0 DISTINCT`
    // is a syntax error. A single select carrying both is reachable through the cubesql
    // wrapper (`SELECT DISTINCT ... LIMIT 0`), which can't be built from here, so the
    // template itself is what gets pinned
    const { select } = query.sqlTemplates().statements;

    expect(select).toContain('DISTINCT');
    expect(select).toContain('TOP');
    expect(select.indexOf('DISTINCT')).toBeLessThan(select.indexOf('TOP'));
  });

  it('keeps rowLimit: 0 out of the legacy limit clauses', async () => {
    await compiler.compile();

    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      timezone: 'UTC',
      rowLimit: 0,
      offset: 10,
    });

    expect(query.topLimit()).toEqual(' TOP 0');
    expect(query.groupByDimensionLimit()).toEqual('');
    // The legacy rollup query in PreAggregations renders no topLimit(), so the zero limit
    // has to come from this hook or that statement would scan the whole rollup
    expect(query.zeroRowLimitTopClause()).toEqual(' TOP 0');
  });

  it('renders no leading zero-limit clause for a non-zero rowLimit', async () => {
    await compiler.compile();

    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      timezone: 'UTC',
      rowLimit: 5,
    });

    expect(query.zeroRowLimitTopClause()).toEqual('');
  });

  it('renders TOP 0 in the legacy-planner pre-aggregation rollup query', async () => {
    // The rollup statement in PreAggregations renders no topLimit(), and T-SQL cannot put a
    // zero limit in a trailing clause, so without zeroRowLimitTopClause() a `rowLimit: 0`
    // query served from a pre-aggregation would scan the whole rollup
    const preAggCompilers = prepareJsCompiler(`
      cube('visits', {
        sql: 'SELECT * FROM visits',

        preAggregations: {
          bySource: {
            measures: [CUBE.count],
            dimensions: [CUBE.source],
          },
        },

        measures: {
          count: { type: 'count' },
        },

        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          source: { sql: 'source', type: 'string' },
        },
      });
    `);
    await preAggCompilers.compiler.compile();

    const queryOptions = {
      measures: ['visits.count'],
      dimensions: ['visits.source'],
      timezone: 'UTC',
      useNativeSqlPlanner: false,
      preAggregationsSchema: '',
    };

    const zeroLimit = new MssqlQuery({
      joinGraph: preAggCompilers.joinGraph,
      cubeEvaluator: preAggCompilers.cubeEvaluator,
      compiler: preAggCompilers.compiler,
    }, { ...queryOptions, rowLimit: 0 });

    const zeroLimitSql = zeroLimit.buildSqlAndParams()[0];

    expect(zeroLimit.preAggregations.findPreAggregationForQuery()).toBeDefined();
    expect(zeroLimitSql).toContain('TOP 0');

    const nonZeroLimit = new MssqlQuery({
      joinGraph: preAggCompilers.joinGraph,
      cubeEvaluator: preAggCompilers.cubeEvaluator,
      compiler: preAggCompilers.compiler,
    }, { ...queryOptions, rowLimit: 5 });

    // Non-zero limits keep their existing rendering on this path
    expect(nonZeroLimit.buildSqlAndParams()[0]).not.toContain('TOP 0');
  });

  it('keeps DISTINCT and TOP 0 in a valid order for a multiplied-measure query', async () => {
    await joinedSchemaCompilers.compiler.compile();

    // Multiplied measures make the full-key-aggregate path emit DISTINCT keys sub-selects
    // alongside the TOP 0 outer select
    const query = new MssqlQuery({
      joinGraph: joinedSchemaCompilers.joinGraph,
      cubeEvaluator: joinedSchemaCompilers.cubeEvaluator,
      compiler: joinedSchemaCompilers.compiler,
    }, {
      measures: ['B.bval_sum', 'C.count'],
      dimensions: ['B.bid'],
      order: [{ id: 'B.bid', desc: false }],
      timezone: 'UTC',
      rowLimit: 0,
    });

    const sql = query.buildSqlAndParams()[0];

    expect(sql).toContain('TOP 0');
    expect(sql).toContain('DISTINCT');
    expect(sql).not.toMatch(/TOP\s+0\s+DISTINCT/);
  });

  it('aggregating on top of sub-queries', async () => {
    await joinedSchemaCompilers.compiler.compile();
    const query = new MssqlQuery({
      joinGraph: joinedSchemaCompilers.joinGraph,
      cubeEvaluator: joinedSchemaCompilers.cubeEvaluator,
      compiler: joinedSchemaCompilers.compiler,
    },
    {
      dimensions: ['E.eval'],
      measures: ['B.bval_sum'],
      order: [{ id: 'B.bval_sum' }],
    });
    const sql = query.buildSqlAndParams();
    // eslint-disable-next-line no-useless-escape
    const re = new RegExp(`(GROUP BY)(\n|.)+("${QueryAlias.AGG_SUB_QUERY_KEYS}"\."e__eval")`);
    expect(re.test(sql[0])).toBeTruthy();
  });
});
