import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// GH #8081: a cube joins another cube on a `sub_query` dimension
// (`propagate_filters_to_sub_query: true`). Selecting a dimension of the joined
// cube works, but filtering on it fails with
// `RangeError: Maximum call stack size exceeded`: the propagated filter needs the
// join, the join needs the sub-query dimension, and the sub-query dimension
// needs the propagated filter again.
describe('Filter through a join on a sub-query dimension (GH #8081)', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
cube('model_a', {
  sql: \`SELECT 60 AS field_1 UNION ALL SELECT 2 AS field_1\`,

  dimensions: {
    field_1: {
      sql: \`field_1\`,
      type: 'number',
      primaryKey: true,
      shown: true,
    },
  },
});

cube('model_b', {
  sql: \`
    SELECT 'r1' AS row_id, 10 AS metric_1 UNION ALL
    SELECT 'r2' AS row_id, 20 AS metric_1 UNION ALL
    SELECT 'r3' AS row_id, 30 AS metric_1
  \`,

  dimensions: {
    cube_unique_id: {
      sql: \`row_id\`,
      type: 'string',
      primaryKey: true,
      shown: true,
    },

    field_1: {
      sql: \`sum(\${metric_1}) over ()\`,
      type: 'number',
      subQuery: true,
      propagateFiltersToSubQuery: true,
    },

    field_2: {
      sql: \`\${model_a.field_1}\`,
      type: 'number',
    },
  },

  measures: {
    metric_1: {
      sql: \`metric_1\`,
      type: 'max',
    },
  },

  joins: {
    model_a: {
      relationship: 'one_to_one',
      sql: \`\${CUBE.field_1} = \${model_a.field_1}\`,
    },
  },
});
`);

  const expected = [
    { model_b__field_2: 60, model_b__cube_unique_id: 'r1' },
    { model_b__field_2: 60, model_b__cube_unique_id: 'r2' },
    { model_b__field_2: 60, model_b__cube_unique_id: 'r3' },
  ];

  // Control: works today.
  it('selects the joined dimension without a filter', async () => dbRunner.runQueryTest({
    dimensions: ['model_b.field_2', 'model_b.cube_unique_id'],
    order: [{ id: 'model_b.field_2' }, { id: 'model_b.cube_unique_id' }],
  }, expected, { joinGraph, cubeEvaluator, compiler }));

  // Repro: exact query from the issue (+ a tie-breaker for deterministic output).
  it('filters on the joined dimension', async () => dbRunner.runQueryTest({
    filters: [{ member: 'model_b.field_2', operator: 'set' }],
    dimensions: ['model_b.field_2', 'model_b.cube_unique_id'],
    order: [{ id: 'model_b.field_2' }, { id: 'model_b.cube_unique_id' }],
  }, expected, { joinGraph, cubeEvaluator, compiler }));
});
