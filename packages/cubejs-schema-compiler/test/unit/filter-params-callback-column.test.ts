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

    // The column applies only what its filter supplies, so an operator carrying
    // no values leaves nothing to apply.
    it('applies nothing when the filter on the column carries no values', async () => {
      const query = await queryFor(useNativeSqlPlanner, {
        dimensions: ['commission.partner', 'commission.pricingDuration'],
        timeDimensions: [],
        filters: [{ member: 'commission.reconciliationDate', operator: 'set' }],
      });
      const [sql] = query.buildSqlAndParams();

      expect(sql).not.toMatch(PUSHED_DOWN_PREDICATE);
      expect(sql).toContain('1 = 1');
    });
  });

  // Fewer values than the column takes would leave its trailing bound unbound.
  // The legacy planner fills that bound in with the current time, quietly
  // widening the predicate to a range the filter never asked for.
  it('reports a filter that supplies fewer values than the column takes', async () => {
    const query = await queryFor(true, {
      dimensions: ['commission.partner', 'commission.pricingDuration'],
      timeDimensions: [],
      filters: [{
        member: 'commission.reconciliationDate',
        operator: 'beforeDate',
        values: ['2025-07-01'],
      }],
    });

    expect(() => query.buildSqlAndParams())
      .toThrow(/takes 2 values but the filter on it supplies 1/);
  });

  // A column renders only where its filter reaches the query, so the cube it
  // reads is needed exactly there — and nowhere else.
  describe.each([
    ['a member of another cube', '${users.city} IS NOT NULL AND ${CUBE.createdAt} >= ${from}'],
    ['another cube directly', '${users}.city IS NOT NULL AND ${CUBE.createdAt} >= ${from}'],
  ])('a column reading %s', (_name, callbackBody) => {
    const schema = [
      'cube(\'orders\', {',
      '  sql: `SELECT * FROM orders`,',
      '  joins: {',
      '    users: {',
      '      sql: `${CUBE}.user_id = ${users}.id`,',
      '      relationship: `belongsTo`',
      '    }',
      '  },',
      '  measures: {',
      '    total: {',
      '      sql: `${CUBE}.amount`,',
      '      type: `sum`,',
      '      filters: [',
      `        { sql: \`\${FILTER_PARAMS.orders.createdAt.filter((from, to) => \`${callbackBody}\`)}\` }`,
      '      ]',
      '    }',
      '  },',
      '  dimensions: {',
      '    id: {',
      '      sql: `id`,',
      '      type: `number`,',
      '      primaryKey: true',
      '    },',
      '    createdAt: {',
      '      sql: `created_at`,',
      '      type: `time`',
      '    }',
      '  }',
      '});',
      'cube(\'users\', {',
      '  sql: `SELECT * FROM users`,',
      '  dimensions: {',
      '    id: {',
      '      sql: `id`,',
      '      type: `number`,',
      '      primaryKey: true',
      '    },',
      '    city: {',
      '      sql: `city`,',
      '      type: `string`',
      '    }',
      '  }',
      '});',
    ].join('\n');

    async function sqlFor(useNativeSqlPlanner: boolean, withFilter: boolean) {
      const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(schema);
      await compiler.compile();
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.total'],
        filters: withFilter
          ? [{ member: 'orders.createdAt', operator: 'inDateRange', values: ['2025-07-01', '2026-06-30'] }]
          : [],
        timezone: 'UTC',
        useNativeSqlPlanner,
      });

      return query.buildSqlAndParams()[0];
    }

    it.each([
      ['legacy planner', false],
      ['native planner', true],
    ])('joins that cube for %s when the filter reaches the query', async (_planner, useNativeSqlPlanner) => {
      const sql = await sqlFor(useNativeSqlPlanner, true);

      expect(sql).toMatch(/join\s+users/i);
      expect(sql).toContain('"users".city IS NOT NULL');
    });

    it.each([
      ['legacy planner', false],
      ['native planner', true],
    ])('leaves that cube out for %s when the filter does not', async (_planner, useNativeSqlPlanner) => {
      const sql = await sqlFor(useNativeSqlPlanner, false);

      expect(sql).not.toMatch(/join\s+users/i);
      expect(sql).toContain('1 = 1');
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
  // way rather than resolved or left to render a qualifier for a table the query
  // does not read.
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

// A column renders wherever the symbol carrying it does, which includes symbols a
// query reaches only through a filter, a segment or an order item. Its cube has
// to be joined in every one of those, or the qualifier it renders has no table
// behind it.
describe('a column reached through something other than a selected member', () => {
  const COLUMN = '${FILTER_PARAMS.orders.createdAt.filter((from, to) => `${users.city} IS NOT NULL AND ${CUBE.createdAt} >= ${from}`)}';

  const schema = [
    'cube(\'orders\', {',
    '  sql: `SELECT * FROM orders`,',
    '  joins: {',
    '    users: {',
    '      sql: `${CUBE}.user_id = ${users}.id`,',
    '      relationship: `belongsTo`',
    '    }',
    '  },',
    '  measures: {',
    '    count: {',
    '      type: `count`',
    '    },',
    `    total: { sql: \`\${CUBE}.amount\`, type: \`sum\`, filters: [{ sql: \`${COLUMN}\` }] },`,
    '    grouped: {',
    '      sql: `${CUBE}.amount`,',
    '      type: `sum`,',
    '      filters: [{ sql: `${FILTER_GROUP(',
    '        FILTER_PARAMS.orders.createdAt.filter((from, to) => `${users.city} IS NOT NULL AND ${CUBE.createdAt} >= ${from}`),',
    '        FILTER_PARAMS.orders.status.filter((v) => `${CUBE.status} = ${v}`)',
    '      )}` }]',
    '    }',
    '  },',
    '  dimensions: {',
    '    id: {',
    '      sql: `id`,',
    '      type: `number`,',
    '      primaryKey: true',
    '    },',
    '    status: {',
    '      sql: `status`,',
    '      type: `string`',
    '    },',
    '    createdAt: {',
    '      sql: `created_at`,',
    '      type: `time`',
    '    },',
    `    flagged: { sql: \`CASE WHEN ${COLUMN} THEN 1 ELSE 0 END\`, type: \`number\` }`,
    '  },',
    `  segments: { recent: { sql: \`${COLUMN}\` } }`,
    '});',
    'cube(\'users\', {',
    '  sql: `SELECT * FROM users`,',
    '  dimensions: {',
    '    id: {',
    '      sql: `id`,',
    '      type: `number`,',
    '      primaryKey: true',
    '    },',
    '    city: {',
    '      sql: `city`,',
    '      type: `string`',
    '    }',
    '  }',
    '});',
  ].join('\n');

  const RANGE = { member: 'orders.createdAt', operator: 'inDateRange', values: ['2025-07-01', '2026-06-30'] };

  async function sqlFor(query: any, useNativeSqlPlanner: boolean) {
    const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(schema);
    await compiler.compile();

    return new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      ...query,
      timezone: 'UTC',
      useNativeSqlPlanner,
    }).buildSqlAndParams()[0];
  }

  it.each([
    ['a segment', { measures: ['orders.count'], segments: ['orders.recent'], filters: [RANGE] }],
    ['a dimension only named in a filter', { measures: ['orders.count'], filters: [RANGE, { member: 'orders.flagged', operator: 'equals', values: ['1'] }] }],
    ['a measure only named in a having filter', { measures: ['orders.count'], filters: [RANGE, { member: 'orders.total', operator: 'gt', values: ['1'] }] }],
    // A FILTER_GROUP renders as one predicate, so its members share one verdict;
    // an OR group survives only when every member of it matches the query.
    ['a filter group under an or filter', { measures: ['orders.grouped'], filters: [{ or: [RANGE, { member: 'orders.status', operator: 'equals', values: ['x'] }] }] }],
  ])('joins the cube read through %s', async (_name, query) => {
    for (const useNativeSqlPlanner of [false, true]) {
      const sql = await sqlFor(query, useNativeSqlPlanner);

      expect(sql).toContain('"users".city IS NOT NULL');
      expect(sql).toMatch(/join\s+users/i);
    }
  });

  // The legacy planner cannot build this one at all: collecting its join hints
  // recurses until the stack runs out.
  it('joins the cube read through a filter group under an and filter', async () => {
    const query = {
      measures: ['orders.grouped'],
      filters: [RANGE, { member: 'orders.status', operator: 'equals', values: ['x'] }],
    };

    const sql = await sqlFor(query, true);

    expect(sql).toContain('"users".city IS NOT NULL');
    expect(sql).toMatch(/join\s+users/i);
  });
});
