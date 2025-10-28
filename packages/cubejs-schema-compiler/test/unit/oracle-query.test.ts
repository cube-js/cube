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

    // Time dimensions without granularity should not appear in GROUP BY
    expect(sql).not.toMatch(/GROUP BY.*created_at/i);
  });

  it('handles time dimension with granularity in SELECT and GROUP BY', async () => {
    await compiler.compile();

    const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
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

    // Time dimension with granularity should appear in SELECT with TRUNC
    expect(sql).toMatch(/TRUNC\(.*created_at/i);
    
    // Time dimension with granularity should appear in GROUP BY
    expect(sql).toMatch(/GROUP BY.*created_at/i);
    
    // Should still have WHERE clause for filtering
    expect(sql).toMatch(/WHERE/i);
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

  describe('addInterval', () => {
    it('adds year interval using ADD_MONTHS', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 year');
      expect(result).toBe('ADD_MONTHS(my_date, 12)');
    });

    it('adds multiple years using ADD_MONTHS', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '3 year');
      expect(result).toBe('ADD_MONTHS(my_date, 36)');
    });

    it('adds month interval using ADD_MONTHS', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 month');
      expect(result).toBe('ADD_MONTHS(my_date, 1)');
    });

    it('adds multiple months using ADD_MONTHS', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '6 month');
      expect(result).toBe('ADD_MONTHS(my_date, 6)');
    });

    it('adds quarter interval using ADD_MONTHS', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 quarter');
      expect(result).toBe('ADD_MONTHS(my_date, 3)');
    });

    it('adds multiple quarters using ADD_MONTHS', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '4 quarter');
      expect(result).toBe('ADD_MONTHS(my_date, 12)');
    });

    it('adds day interval using NUMTODSINTERVAL', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 day');
      expect(result).toBe('my_date + NUMTODSINTERVAL(1, \'DAY\')');
    });

    it('adds multiple days using NUMTODSINTERVAL', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '7 day');
      expect(result).toBe('my_date + NUMTODSINTERVAL(7, \'DAY\')');
    });

    it('adds hour interval using NUMTODSINTERVAL', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 hour');
      expect(result).toBe('my_date + NUMTODSINTERVAL(1, \'HOUR\')');
    });

    it('adds multiple hours using NUMTODSINTERVAL', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '24 hour');
      expect(result).toBe('my_date + NUMTODSINTERVAL(24, \'HOUR\')');
    });

    it('adds minute interval using NUMTODSINTERVAL', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 minute');
      expect(result).toBe('my_date + NUMTODSINTERVAL(1, \'MINUTE\')');
    });

    it('adds multiple minutes using NUMTODSINTERVAL', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '30 minute');
      expect(result).toBe('my_date + NUMTODSINTERVAL(30, \'MINUTE\')');
    });

    it('adds second interval using NUMTODSINTERVAL', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 second');
      expect(result).toBe('my_date + NUMTODSINTERVAL(1, \'SECOND\')');
    });

    it('adds multiple seconds using NUMTODSINTERVAL', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '45 second');
      expect(result).toBe('my_date + NUMTODSINTERVAL(45, \'SECOND\')');
    });

    it('combines year and month into single ADD_MONTHS', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 year 6 month');
      expect(result).toBe('ADD_MONTHS(my_date, 18)');
    });

    it('combines quarter and month into single ADD_MONTHS', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '2 quarter 3 month');
      expect(result).toBe('ADD_MONTHS(my_date, 9)');
    });

    it('combines year, quarter, and month into single ADD_MONTHS', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '2 year 1 quarter 2 month');
      expect(result).toBe('ADD_MONTHS(my_date, 29)');
    });

    it('combines day and hour intervals', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 day 2 hour');
      expect(result).toBe('my_date + NUMTODSINTERVAL(1, \'DAY\') + NUMTODSINTERVAL(2, \'HOUR\')');
    });

    it('combines hour, minute, and second intervals', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 hour 30 minute 45 second');
      expect(result).toBe('my_date + NUMTODSINTERVAL(1, \'HOUR\') + NUMTODSINTERVAL(30, \'MINUTE\') + NUMTODSINTERVAL(45, \'SECOND\')');
    });

    it('combines month-based and day-based intervals', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 year 2 day 3 hour');
      expect(result).toBe('ADD_MONTHS(my_date, 12) + NUMTODSINTERVAL(2, \'DAY\') + NUMTODSINTERVAL(3, \'HOUR\')');
    });

    it('combines all interval types', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('my_date', '1 year 2 quarter 3 month 4 day 5 hour 6 minute 7 second');
      expect(result).toBe('ADD_MONTHS(my_date, 21) + NUMTODSINTERVAL(4, \'DAY\') + NUMTODSINTERVAL(5, \'HOUR\') + NUMTODSINTERVAL(6, \'MINUTE\') + NUMTODSINTERVAL(7, \'SECOND\')');
    });

    it('handles complex date expressions', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.addInterval('TRUNC(my_date)', '1 month');
      expect(result).toBe('ADD_MONTHS(TRUNC(my_date), 1)');
    });
  });

  describe('subtractInterval', () => {
    it('subtracts year interval using ADD_MONTHS with negative value', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 year');
      expect(result).toBe('ADD_MONTHS(my_date, -12)');
    });

    it('subtracts multiple years using ADD_MONTHS with negative value', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '3 year');
      expect(result).toBe('ADD_MONTHS(my_date, -36)');
    });

    it('subtracts month interval using ADD_MONTHS with negative value', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 month');
      expect(result).toBe('ADD_MONTHS(my_date, -1)');
    });

    it('subtracts multiple months using ADD_MONTHS with negative value', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '6 month');
      expect(result).toBe('ADD_MONTHS(my_date, -6)');
    });

    it('subtracts quarter interval using ADD_MONTHS with negative value', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 quarter');
      expect(result).toBe('ADD_MONTHS(my_date, -3)');
    });

    it('subtracts multiple quarters using ADD_MONTHS with negative value', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '4 quarter');
      expect(result).toBe('ADD_MONTHS(my_date, -12)');
    });

    it('subtracts day interval using NUMTODSINTERVAL subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 day');
      expect(result).toBe('my_date - NUMTODSINTERVAL(1, \'DAY\')');
    });

    it('subtracts multiple days using NUMTODSINTERVAL subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '7 day');
      expect(result).toBe('my_date - NUMTODSINTERVAL(7, \'DAY\')');
    });

    it('subtracts hour interval using NUMTODSINTERVAL subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 hour');
      expect(result).toBe('my_date - NUMTODSINTERVAL(1, \'HOUR\')');
    });

    it('subtracts multiple hours using NUMTODSINTERVAL subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '24 hour');
      expect(result).toBe('my_date - NUMTODSINTERVAL(24, \'HOUR\')');
    });

    it('subtracts minute interval using NUMTODSINTERVAL subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 minute');
      expect(result).toBe('my_date - NUMTODSINTERVAL(1, \'MINUTE\')');
    });

    it('subtracts multiple minutes using NUMTODSINTERVAL subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '30 minute');
      expect(result).toBe('my_date - NUMTODSINTERVAL(30, \'MINUTE\')');
    });

    it('subtracts second interval using NUMTODSINTERVAL subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 second');
      expect(result).toBe('my_date - NUMTODSINTERVAL(1, \'SECOND\')');
    });

    it('subtracts multiple seconds using NUMTODSINTERVAL subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '45 second');
      expect(result).toBe('my_date - NUMTODSINTERVAL(45, \'SECOND\')');
    });

    it('combines year and month into single ADD_MONTHS with negative value', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 year 6 month');
      expect(result).toBe('ADD_MONTHS(my_date, -18)');
    });

    it('combines quarter and month into single ADD_MONTHS with negative value', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '2 quarter 3 month');
      expect(result).toBe('ADD_MONTHS(my_date, -9)');
    });

    it('combines year, quarter, and month into single ADD_MONTHS with negative value', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '2 year 1 quarter 2 month');
      expect(result).toBe('ADD_MONTHS(my_date, -29)');
    });

    it('combines day and hour intervals with subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 day 2 hour');
      expect(result).toBe('my_date - NUMTODSINTERVAL(1, \'DAY\') - NUMTODSINTERVAL(2, \'HOUR\')');
    });

    it('combines hour, minute, and second intervals with subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 hour 30 minute 45 second');
      expect(result).toBe('my_date - NUMTODSINTERVAL(1, \'HOUR\') - NUMTODSINTERVAL(30, \'MINUTE\') - NUMTODSINTERVAL(45, \'SECOND\')');
    });

    it('combines month-based and day-based intervals with subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 year 2 day 3 hour');
      expect(result).toBe('ADD_MONTHS(my_date, -12) - NUMTODSINTERVAL(2, \'DAY\') - NUMTODSINTERVAL(3, \'HOUR\')');
    });

    it('combines all interval types with subtraction', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('my_date', '1 year 2 quarter 3 month 4 day 5 hour 6 minute 7 second');
      expect(result).toBe('ADD_MONTHS(my_date, -21) - NUMTODSINTERVAL(4, \'DAY\') - NUMTODSINTERVAL(5, \'HOUR\') - NUMTODSINTERVAL(6, \'MINUTE\') - NUMTODSINTERVAL(7, \'SECOND\')');
    });

    it('handles complex date expressions', async () => {
      await compiler.compile();
      const query = new OracleQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.count'],
        timezone: 'UTC'
      });
      const result = query.subtractInterval('TRUNC(my_date)', '1 month');
      expect(result).toBe('ADD_MONTHS(TRUNC(my_date), -1)');
    });
  });

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
