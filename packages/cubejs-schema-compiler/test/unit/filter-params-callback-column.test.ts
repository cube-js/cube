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
