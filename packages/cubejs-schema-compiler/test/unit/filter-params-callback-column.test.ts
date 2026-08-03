/* eslint-disable no-template-curly-in-string */
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from './PrepareCompiler';

// A `FILTER_PARAMS.….filter(cb)` column callback runs at render time, and the
// member references inside it must resolve to that member's SQL — the same as
// when the column is handed over as a template string. Here the callback is the
// only place referencing the time dimension, and it sits in a measure `filters:`
// entry, so nothing else in that SQL function records the reference.
const model = [
  'cube(\'commission\', {',
  '  sql: `SELECT * FROM commission`,',
  '  measures: {',
  '    dailyMrr: {',
  '      sql: `${CUBE}.total`,',
  '      type: `sum`,',
  '      filters: [',
  '        { sql: `${CUBE.pricingDuration} = \'DAILY\'` },',
  '        { sql: `${FILTER_PARAMS.commission.reconciliationDate.filter((from, to) => `${CUBE.reconciliationDate} >= ${from} AND ${CUBE.reconciliationDate} < ${to}`)}` }',
  '      ]',
  '    }',
  '  },',
  '  dimensions: {',
  '    id: {',
  '      sql: `id`,',
  '      type: `number`,',
  '      primaryKey: true',
  '    },',
  '    partner: {',
  '      sql: `partner`,',
  '      type: `string`',
  '    },',
  '    currency: {',
  '      sql: `currency`,',
  '      type: `string`',
  '    },',
  '    pricingDuration: {',
  '      sql: `pricing_duration`,',
  '      type: `string`',
  '    },',
  '    reconciliationDate: {',
  '      sql: `reconciliation_date`,',
  '      type: `time`',
  '    }',
  '  },',
  '  preAggregations: {',
  '    main: {',
  '      measures: [CUBE.dailyMrr],',
  '      dimensions: [CUBE.partner, CUBE.currency],',
  '      timeDimension: CUBE.reconciliationDate,',
  '      granularity: `month`,',
  '      partitionGranularity: `year`',
  '    }',
  '  }',
  '});',
].join('\n');

// The callback's own SQL, with the time dimension resolved and the date bounds
// bound as params.
const PUSHED_DOWN_PREDICATE = /"commission"\.reconciliation_date >= \$\d+ AND "commission"\.reconciliation_date < \$\d+/;

async function queryFor(useNativeSqlPlanner: boolean, options = {}) {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model);
  await compiler.compile();

  return new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['commission.dailyMrr'],
    dimensions: ['commission.partner'],
    filters: [{
      member: 'commission.currency',
      operator: 'equals',
      values: ['USD'],
    }],
    timeDimensions: [{
      dimension: 'commission.reconciliationDate',
      granularity: 'month',
      dateRange: ['2025-07-01', '2026-06-30'],
    }],
    timezone: 'UTC',
    useNativeSqlPlanner,
    ...options,
  });
}

describe('FILTER_PARAMS callback column', () => {
  describe.each([
    ['legacy planner', false],
    ['native planner', true],
  ])('%s', (_name, useNativeSqlPlanner) => {
    it('renders the callback of a measure filter against the cube table', async () => {
      // `pricingDuration` is outside the pre-aggregation, so the query reads the
      // cube itself and the measure filter is rendered.
      const query = await queryFor(useNativeSqlPlanner, {
        dimensions: ['commission.partner', 'commission.pricingDuration'],
      });
      const [sql] = query.buildSqlAndParams();

      expect(sql).toMatch(PUSHED_DOWN_PREDICATE);
    });

    it('renders the callback of a measure filter in a pre-aggregation build query', async () => {
      const query = await queryFor(useNativeSqlPlanner);
      const [description]: any = query.preAggregations?.preAggregationsDescription();
      const [loadSql] = description.loadSql;

      expect(loadSql).toMatch(PUSHED_DOWN_PREDICATE);
    });
  });
});

// A cube's `sql` builds the table the query reads from, so nothing a member
// reference could resolve against is in scope inside it — whichever way the
// reference is written.
describe('member references in a cube\'s sql', () => {
  const cubeWith = (cubeSql: string, extraCube = '') => [
    extraCube,
    'cube(\'commission\', {',
    `  sql: \`${cubeSql}\`,`,
    '  measures: {',
    '    total: {',
    '      sql: `${CUBE}.total`,',
    '      type: `sum`',
    '    }',
    '  },',
    '  dimensions: {',
    '    id: {',
    '      sql: `id`,',
    '      type: `number`,',
    '      primaryKey: true',
    '    },',
    '    reconciliationDate: {',
    '      sql: `reconciliation_date`,',
    '      type: `time`',
    '    }',
    '  }',
    '});',
  ].join('\n');

  async function sqlFor(schema: string, withFilter: boolean) {
    const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(schema);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['commission.total'],
      timeDimensions: withFilter
        ? [{ dimension: 'commission.reconciliationDate', dateRange: ['2025-07-01', '2026-06-30'] }]
        : [],
      timezone: 'UTC',
      contextSymbols: { securityContext: { tenantId: 'acme' } },
      useNativeSqlPlanner: true,
    });

    return query.buildSqlAndParams()[0];
  }

  const REJECTED = /references member `commission\.reconciliationDate`/;

  // Each spelling reaches the same place, so each has to be reported the same
  // way rather than resolved, left to render an out-of-scope qualifier, or —
  // for the forms that used to recurse — abandoned on a blown stack.
  it.each([
    ['a direct reference', 'SELECT * FROM commission WHERE ${CUBE.reconciliationDate} IS NOT NULL'],
    ['a string filter param column', 'SELECT * FROM commission WHERE ${FILTER_PARAMS.commission.reconciliationDate.filter(`${CUBE.reconciliationDate}`)}'],
    ['a filter param column callback', 'SELECT * FROM commission WHERE ${FILTER_PARAMS.commission.reconciliationDate.filter((from, to) => `${CUBE.reconciliationDate} >= ${from}`)}'],
  ])('rejects %s', async (_name, cubeSql) => {
    // Reported whether or not the query supplies the filter, since resolving the
    // reference is what a cube's sql cannot do at all.
    await expect(sqlFor(cubeWith(cubeSql), false)).rejects.toThrow(REJECTED);
    await expect(sqlFor(cubeWith(cubeSql), true)).rejects.toThrow(REJECTED);
  });

  it('keeps a filter param column that reads plain columns', async () => {
    const schema = cubeWith('SELECT * FROM commission WHERE ${FILTER_PARAMS.commission.reconciliationDate.filter((from, to) => `reconciliation_date >= ${from} AND reconciliation_date < ${to}`)}');

    expect(await sqlFor(schema, false)).toContain('WHERE 1 = 1');
    expect(await sqlFor(schema, true)).toMatch(/reconciliation_date >= \$\d+ AND reconciliation_date < \$\d+/);
  });

  // A security context value needs no member in scope — it becomes a query
  // param — so the row-level-security shape keeps working.
  it('keeps a security context value inside a filter param column', async () => {
    const schema = cubeWith('SELECT * FROM commission WHERE ${FILTER_PARAMS.commission.reconciliationDate.filter((from, to) => `reconciliation_date >= ${from} AND tenant = ${SECURITY_CONTEXT.tenantId.unsafeValue()}`)}');

    expect(await sqlFor(schema, true)).toContain('tenant = acme');
  });

  it('keeps a reference to another cube\'s sql', async () => {
    const base = [
      'cube(\'base\', {',
      '  sql: `SELECT * FROM raw`,',
      '  dimensions: {',
      '    id: {',
      '      sql: `id`,',
      '      type: `number`,',
      '      primaryKey: true',
      '    }',
      '  }',
      '});',
    ].join('\n');

    expect(await sqlFor(cubeWith('SELECT * FROM ${base.sql()}', base), false)).toMatch(/FROM\s+raw\b/);
  });
});
