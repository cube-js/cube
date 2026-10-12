import { prepareJsCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';

// https://github.com/cube-js/cube/issues/8081
// `model_b` joins `model_a` on a `sub_query` dimension that has
// `propagate_filters_to_sub_query: true`. Filtering on `model_b.field_2`
// (= `model_a.field_1`) propagates that filter into the sub-query, which then
// needs the join to `model_a`, whose condition needs the sub-query dimension
// again, and so on: `RangeError: Maximum call stack size exceeded`.
// Selecting `field_2` without the filter works.
describe('Issue #8081: filter on a member reached through a join on a sub_query dimension', () => {
  const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(`
cube('model_a', {
  sql: 'select 1 as field_1',
  dimensions: {
    field_1: { sql: 'field_1', type: 'string' },
  },
});

cube('model_b', {
  sql: 'select 1 as row_id, 1 as metric_1',
  dimensions: {
    cube_unique_id: { sql: 'row_id', type: 'string', primary_key: true },
    field_1: {
      sql: \`sum(\${metric_1}) over ()\`,
      type: 'number',
      sub_query: true,
      propagate_filters_to_sub_query: true,
    },
    field_2: { sql: \`\${model_a.field_1}\`, type: 'string' },
  },
  measures: {
    metric_1: { sql: 'metric_1', type: 'max' },
  },
  joins: {
    model_a: {
      relationship: 'one_to_one',
      sql: \`\${CUBE.field_1} = \${model_a.field_1}\`,
    },
  },
});
`);

  const build = (filters: any[]) => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['model_b.field_2', 'model_b.cube_unique_id'],
      filters,
      order: [{ id: 'model_b.field_2' }],
      timezone: 'UTC',
    });
    return query.buildSqlAndParams();
  };

  // Control: works today.
  it('selects the joined dimension without a filter', async () => {
    await compiler.compile();
    expect(() => build([])).not.toThrow();
  });

  it('filters on the joined dimension', async () => {
    await compiler.compile();
    expect(() => build([{ member: 'model_b.field_2', operator: 'set' }])).not.toThrow();
  });
});
