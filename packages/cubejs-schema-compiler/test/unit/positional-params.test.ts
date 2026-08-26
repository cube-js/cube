import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { allDialects as allDialectsWithNames } from './allDialects';
import { prepareJsCompiler } from './PrepareCompiler';

// `?` placeholders are positional: a value referenced from two places in the
// generated SQL needs one entry in the params array per placeholder. A security
// context value used twice inside a single member's SQL is the shortest way to
// get such a repeated reference — the value is recorded once and the same
// placeholder is spliced at both occurrences.
const model = [
  'cube(\'orders\', {',
  // eslint-disable-next-line no-template-curly-in-string
  '  sql: `SELECT * FROM orders WHERE ${SECURITY_CONTEXT.tenantId.filter(t => `(tenant_id = ${t} OR parent_tenant_id = ${t})`)}`,',
  '  measures: {',
  '    count: {',
  '      type: `count`',
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
  '  },',
  '  preAggregations: {',
  '    main: {',
  '      measures: [CUBE.count],',
  '      timeDimension: CUBE.createdAt,',
  '      granularity: `day`,',
  '      partitionGranularity: `month`',
  '    }',
  '  }',
  '});',
].join('\n');

async function queryFor(QueryClass, useNativeSqlPlanner: boolean, options = {}) {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model);
  await compiler.compile();

  return new QueryClass({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['orders.count'],
    timezone: 'UTC',
    contextSymbols: {
      securityContext: { tenantId: 'acme' },
    },
    useNativeSqlPlanner,
    ...options,
  });
}

function bigQueryFor(useNativeSqlPlanner: boolean, options = {}) {
  return queryFor(BigqueryQuery, useNativeSqlPlanner, options);
}

function placeholdersCount(sql: string) {
  return (sql.match(/\?/g) || []).length;
}

describe('positional params', () => {
  // Dialects whose placeholder carries the param index are free to share a param
  // between placeholders; those rendering a bare placeholder are not, since the
  // placeholder then says nothing about which value it binds.
  it('never reuses params on dialects whose placeholder omits the param index', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model);
    await compiler.compile();

    const allDialects = () => allDialectsWithNames().map(([, QueryClass]) => QueryClass);

    const reusingPositionalDialects = allDialects().filter(QueryClass => {
      const query = new QueryClass({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timezone: 'UTC',
      });
      const indexedPlaceholder = query.sqlTemplates().params.param.includes('param_index');

      return !indexedPlaceholder && query.shouldReuseParams;
    }).map(QueryClass => QueryClass.name);

    expect(reusingPositionalDialects).toEqual([]);
  });

  describe.each([
    ['legacy planner', false],
    ['native planner', true],
  ])('%s', (_name, useNativeSqlPlanner) => {
    it('allocates a param per placeholder in a query', async () => {
      const query = await bigQueryFor(useNativeSqlPlanner, { dimensions: ['orders.id'] });
      const [sql, params] = query.buildSqlAndParams();

      expect(placeholdersCount(sql)).toEqual(params.length);
      expect(params).toEqual(['acme', 'acme']);
    });

    it('shares one param between placeholders when the placeholder carries its index', async () => {
      const query = await queryFor(PostgresQuery, useNativeSqlPlanner, { dimensions: ['orders.id'] });
      const [sql, params] = query.buildSqlAndParams();

      // `$1` names the value it binds, so both occurrences can point at it.
      expect((sql.match(/\$1\b/g) || []).length).toEqual(2);
      expect(params).toEqual(['acme']);
    });

    it('allocates a param per placeholder in a pre-aggregation build query', async () => {
      const query = await bigQueryFor(useNativeSqlPlanner, {
        timeDimensions: [{
          dimension: 'orders.createdAt',
          granularity: 'day',
          dateRange: ['2024-01-01', '2024-01-31'],
        }],
      });
      const [description]: any = query.preAggregations?.preAggregationsDescription();
      const [loadSql, params] = description.loadSql;

      expect(placeholdersCount(loadSql)).toEqual(params.length);
      expect(params).toEqual(['acme', 'acme', '__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']);
    });
  });
});
