import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from './PrepareCompiler';

// #11124: a cross-data-source `rollup_join` that lists a measure of the
// right-side (secondary) cube does not serve a query that includes that measure.
// - Legacy planner: the rollup_join is not matched (preAggregationForQuery is
//   undefined) and the query falls through to
//   "To join across data sources use rollupJoin with Cube Store".
// - Tesseract: the rollup_join is matched, but the SQL reads
//   `primary_cube_primary_rollup` / `secondary_cube_main` (no usage suffix) and
//   `secondary_cube_main__usage_0`, while the pre-aggregation descriptions only
//   carry usageMapping `__usage_1`, so the orchestrator cannot substitute the
//   built tables ("table never built" at query time, as reported on v1.7.4).
// The query without the secondary measure (control) works on both planners.
describe('Issue #11124 - rollup_join with a measure from the right-side cube', () => {
  const model = `
cube('SecondaryCube', {
  data_source: 'secondary',
  sql: \`select 1 as id, 'loc_1' as location_id, 500 as contract_value\`,
  dimensions: {
    id: { sql: 'id', type: 'string', primary_key: true },
    location_id: { sql: 'location_id', type: 'string' },
  },
  measures: {
    total_contract_value: { sql: 'contract_value', type: 'sum' },
  },
  pre_aggregations: {
    main: {
      type: 'rollup',
      external: true,
      measures: [CUBE.total_contract_value],
      dimensions: [CUBE.id, CUBE.location_id],
    },
  },
});

cube('PrimaryCube', {
  sql: \`select 1 as id, 1 as secondary_id, 'SUCCESS' as action_status\`,
  joins: {
    SecondaryCube: {
      sql: \`\${CUBE.secondary_id} = \${SecondaryCube.id}\`,
      relationship: 'many_to_one',
    },
  },
  dimensions: {
    id: { sql: 'id', type: 'string', primary_key: true },
    secondary_id: { sql: 'secondary_id', type: 'string' },
    action_status: { sql: 'action_status', type: 'string' },
  },
  measures: {
    total_count: {
      type: 'count',
      filters: [{ sql: \`\${CUBE}.action_status = 'SUCCESS'\` }],
    },
  },
  pre_aggregations: {
    primary_rollup: {
      type: 'rollup',
      external: true,
      measures: [CUBE.total_count],
      dimensions: [CUBE.secondary_id, CUBE.action_status],
    },
    joined_rollup: {
      type: 'rollup_join',
      rollups: [PrimaryCube.primary_rollup, SecondaryCube.main],
      measures: [PrimaryCube.total_count, PrimaryCube.SecondaryCube.total_contract_value],
      dimensions: [PrimaryCube.secondary_id, PrimaryCube.SecondaryCube.location_id],
    },
  },
});
`;

  const buildQuery = async (measures: string[]) => {
    const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model);
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures,
      dimensions: ['SecondaryCube.location_id'],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });
    const [sql] = query.buildSqlAndParams();
    return { query, sql };
  };

  // Every pre-aggregation table the SQL reads must be resolvable by the query
  // orchestrator: it substitutes `tableName + usageMapping suffix` (or the bare
  // `tableName` when there is no usageMapping) with the built table.
  const expectTablesResolvable = (query: PostgresQuery, sql: string) => {
    const descriptions: any[] = query.preAggregations?.preAggregationsDescription() || [];
    const resolvable = new Set<string>();

    for (const d of descriptions) {
      if (d.usageMapping) {
        Object.keys(d.usageMapping).forEach(suffix => resolvable.add(`${d.tableName}${suffix}`));
      } else {
        resolvable.add(d.tableName);
      }
    }
    const referenced = Array.from(new Set(sql.match(/\b(?:primary_cube_primary_rollup|secondary_cube_main)(?:__usage_\d+)?\b/g) || []));
    expect(referenced.length).toBeGreaterThan(0);
    expect(referenced.filter(t => !resolvable.has(t))).toEqual([]);
  };

  it('control: primary-cube measure + secondary-cube dimension uses the rollup_join', async () => {
    const { query, sql } = await buildQuery(['PrimaryCube.total_count']);
    expect(query.preAggregations?.preAggregationForQuery?.preAggregationName).toEqual('joined_rollup');
    expect(sql).toContain('primary_cube_primary_rollup');
    expect(sql).toContain('secondary_cube_main');
    expectTablesResolvable(query, sql);
  });

  it('adding a secondary-cube measure listed in the rollup_join still uses it', async () => {
    const { query, sql } = await buildQuery(['PrimaryCube.total_count', 'SecondaryCube.total_contract_value']);
    expect(query.preAggregations?.preAggregationForQuery?.preAggregationName).toEqual('joined_rollup');
    expect(sql).toContain('primary_cube_primary_rollup');
    expect(sql).toContain('secondary_cube_main');
    expectTablesResolvable(query, sql);
  });
});
