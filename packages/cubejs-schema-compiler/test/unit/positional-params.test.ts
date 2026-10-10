import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { ClickHouseQuery } from '../../src/adapter/ClickHouseQuery';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { allDialects } from './allDialects';
import { prepareJsCompiler } from './PrepareCompiler';

// A regex literal with question marks that are not placeholders
const CODE_REGEX = '\'(.*?)(?:-?[0-9]{2})\'';

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
  '    },',
  '    code: {',
  `      sql: \`extract(code, ${CODE_REGEX})\`,`,
  '      type: `string`',
  '    }',
  '  },',
  '  preAggregations: {',
  '    main: {',
  '      measures: [CUBE.count],',
  '      timeDimension: CUBE.createdAt,',
  '      granularity: `day`,',
  '      partitionGranularity: `month`,',
  // ClickHouse requires an index on pre-aggregations
  '      indexes: { byCreatedAt: { columns: [CUBE.createdAt.day] } }',
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

function clickHouseTokens(sql: string) {
  return sql.match(/___ClickHouseParam_\d+___/g) || [];
}

describe('positional params', () => {
  // Dialects whose placeholder carries the param index are free to share a param
  // between placeholders; those rendering a bare placeholder are not, since the
  // placeholder then says nothing about which value it binds.
  it('never reuses params on dialects whose placeholder omits the param index', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model);
    await compiler.compile();

    const reusingPositionalDialects = allDialects().filter(([, QueryClass]) => {
      const query = new QueryClass({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timezone: 'UTC',
      });
      const indexedPlaceholder = query.sqlTemplates().params.param.includes('param_index');

      return !indexedPlaceholder && query.shouldReuseParams;
    }).map(([name]) => name);

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

    // ClickHouse binds params by token, so a literal `?` in member SQL is not a placeholder
    it('renders ClickHouse params as indexed tokens', async () => {
      const query = await queryFor(ClickHouseQuery, useNativeSqlPlanner, {
        dimensions: ['orders.code'],
        filters: [{ member: 'orders.code', operator: 'equals', values: ['x'] }],
      });
      const [sql, params] = query.buildSqlAndParams();

      expect(sql).toContain(CODE_REGEX);
      expect(placeholdersCount(sql.split(CODE_REGEX).join(''))).toEqual(0);
      expect(clickHouseTokens(sql)).toEqual(params.map((_, i) => `___ClickHouseParam_${i}___`));
      expect(params).toEqual(['acme', 'acme', 'x']);
    });

    it('renders ClickHouse params as indexed tokens in a pre-aggregation build query', async () => {
      const query = await queryFor(ClickHouseQuery, useNativeSqlPlanner, {
        timeDimensions: [{
          dimension: 'orders.createdAt',
          granularity: 'day',
          dateRange: ['2024-01-01', '2024-01-31'],
        }],
      });
      const [description]: any = query.preAggregations?.preAggregationsDescription();
      const [loadSql, params] = description.loadSql;

      expect(placeholdersCount(loadSql)).toEqual(0);
      expect(clickHouseTokens(loadSql)).toEqual(params.map((_, i) => `___ClickHouseParam_${i}___`));
      expect(params).toEqual(['acme', 'acme', '__FROM_PARTITION_RANGE', '__TO_PARTITION_RANGE']);
    });
  });
});
