import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A `rank` multi-stage measure whose `order_by` template references another
// measure by its bare name (`{num_parcels}`) must resolve that reference against
// the measure's owning cube, not against a view exposing the rank. Otherwise the
// order_by template is evaluated in the view's context during the shared JS
// member-collection pass and crashes with
// `Cannot read properties of undefined (reading '_objectWithResolvedProperties')`
// (the referenced member is absent from the view namespace, or present only under
// an alias).
describe('Multi-stage measure order_by through a view', () => {
  const model = (viewIncludes: string) => `
cubes:
  - name: orders
    sql: "SELECT 1 AS id, 10 AS parcels, '2024-01-01'::timestamptz AS created_at"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: num_parcels
        type: sum
        sql: parcels
      - name: volume_by_day_rank
        type: rank
        multi_stage: true
        reduce_by:
          - created_at
        order_by:
          - sql: "{num_parcels}"
            dir: desc

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
${viewIncludes}
  `;

  const buildViewRankQuery = async (viewIncludes: string) => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model(viewIncludes));
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['orders_view.volume_by_day_rank'],
      timeDimensions: [{ dimension: 'orders_view.created_at', granularity: 'day' }],
      order: [{ id: 'orders_view.created_at' }],
      timezone: 'UTC',
      // The crash is in the shared JS member-collection pass (query construction),
      // which runs under both planners; pin the planner so the asserted SQL is
      // deterministic and does not require the native addon to be built.
      useNativeSqlPlanner: false,
    });
    return query.buildSqlAndParams();
  };

  it('resolves order_by against the owning cube when the member is not exposed by the view', async () => {
    const [sql] = await buildViewRankQuery(
      '          - created_at\n          - volume_by_day_rank'
    );
    expect(sql).toContain('rank() OVER (');
    expect(sql).toContain('"orders__num_parcels" desc');
  });

  it('resolves order_by against the owning cube when the member is exposed under an alias', async () => {
    const [sql] = await buildViewRankQuery(
      '          - created_at\n          - volume_by_day_rank\n          - name: num_parcels\n            alias: total_parcels'
    );
    expect(sql).toContain('rank() OVER (');
    expect(sql).toContain('"orders__num_parcels" desc');
  });
});
