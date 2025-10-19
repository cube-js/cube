/* eslint-disable no-restricted-syntax */
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
        },

        unboundedCount: {
          type: 'count',
          rollingWindow: {
            trailing: 'unbounded'
          }
        },

        thisPeriod: {
          sql: 'amount',
          type: 'sum',
          rollingWindow: {
            trailing: '1 year',
            offset: 'end'
          }
        },

        priorPeriod: {
          sql: 'amount',
          type: 'sum',
          rollingWindow: {
            trailing: '1 year',
            offset: 'start'
          }
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
        },

        source: {
          type: 'string',
          sql: 'source'
        }
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
    `, { adapter: 'oracle' });

  it('basic query without subqueries', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Basic query should work
    expect(sql).toContain('SELECT');
    expect(sql).toMatch(/FROM\s+visitors/i);
    // Should not have subquery aliases in simple query
    expect(sql).not.toMatch(/\bq_\d+\b/);
    // Should use Oracle FETCH NEXT
    expect(sql).toContain('FETCH NEXT');
  });

  it('does not use AS keyword in subquery aliases with single rolling window', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count',
        'visitors.unboundedCount'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2020-01-01', '2020-01-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Oracle should NOT have AS keyword before subquery aliases
    expect(sql).not.toMatch(/\bAS\s+q_\d+/i);
    expect(sql).not.toMatch(/\bas\s+q_\d+/);
    
    // Should have q_0 alias (with space around it, indicating no AS)
    expect(sql).toMatch(/\)\s+q_0\s+/);
  });

  it('does not use AS keyword with multiple rolling window measures (YoY scenario)', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.thisPeriod',
        'visitors.priorPeriod'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'year',
        dateRange: ['2020-01-01', '2022-12-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Should have multiple subquery aliases (q_0, q_1, q_2, etc.)
    expect(sql).toMatch(/\bq_0\b/);
    expect(sql).toMatch(/\bq_1\b/);
    
    // Oracle should NOT have AS keyword anywhere before q_ aliases
    expect(sql).not.toMatch(/\bAS\s+q_\d+/i);
    expect(sql).not.toMatch(/\bas\s+q_\d+/);
    
    // Verify pattern is ) q_X not ) AS q_X
    expect(sql).toMatch(/\)\s+q_\d+/);
  });

  it('does not use AS keyword in INNER JOIN subqueries', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: [
        'SalesManagers.id',
        'SalesManagers.dealsAmount'
      ]
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Should have INNER JOIN for subquery dimension
    if (sql.includes('INNER JOIN')) {
      // Oracle should NOT have AS keyword in INNER JOIN
      expect(sql).not.toMatch(/INNER\s+JOIN\s+\([^)]+\)\s+AS\s+/i);
      expect(sql).not.toMatch(/INNER\s+JOIN\s+\([^)]+\)\s+as\s+/);
    }
  });

  it('uses FETCH NEXT syntax instead of LIMIT', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timezone: 'UTC',
      limit: 100
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Oracle should use FETCH NEXT instead of LIMIT
    expect(sql).toContain('FETCH NEXT');
    expect(sql).toContain('ROWS ONLY');
    expect(sql).not.toContain('LIMIT');
  });

  it('uses FETCH NEXT syntax with subqueries and rolling windows', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.thisPeriod',
        'visitors.priorPeriod'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'month',
        dateRange: ['2020-01-01', '2020-12-31']
      }],
      timezone: 'UTC',
      limit: 50
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Should have subqueries without AS
    expect(sql).not.toMatch(/\bAS\s+q_\d+/i);
    
    // Should use Oracle-specific FETCH NEXT
    expect(sql).toContain('FETCH NEXT');
    expect(sql).toContain('ROWS ONLY');
    expect(sql).not.toContain('LIMIT');
  });

  it('does not use AS keyword with comma-separated subqueries', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.thisPeriod',
        'visitors.priorPeriod'
      ],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Should have multiple subquery aliases
    expect(sql).toMatch(/\)\s+q_0\s+,/);
    expect(sql).toMatch(/\)\s+q_1\s+/);
    
    // Should NOT have AS before q_ aliases
    expect(sql).not.toMatch(/\bAS\s+q_\d+/i);
    expect(sql).not.toMatch(/\bas\s+q_\d+/);
  });

  it('group by dimensions not indexes', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Oracle should group by actual dimension SQL, not by index
    expect(sql).toMatch(/GROUP BY.*"visitors"\.source/i);
    expect(sql).not.toMatch(/GROUP BY\s+\d+/);
  });

  it('handles time dimension without granularity in filter', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        dateRange: ['2020-01-01', '2020-12-31']
        // No granularity specified - used only for filtering
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Key test: no GROUP BY on time dimension when granularity is missing
    expect(sql).not.toMatch(/GROUP BY.*created_at/i);
  });

  it('uses Oracle-specific interval arithmetic', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.thisPeriod',
        'visitors.priorPeriod'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'year',
        dateRange: ['2020-01-01', '2022-12-31']
      }],
      timezone: 'UTC'
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    // Key test: Oracle uses ADD_MONTHS, not PostgreSQL interval syntax
    expect(sql).toMatch(/ADD_MONTHS/i);
    expect(sql).not.toMatch(/interval '1 year'/i);
  });
});
