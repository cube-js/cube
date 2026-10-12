import YAML from 'yaml';

import { CubePreAggregationConverter, CubeSchemaConverter } from '../../src';

// https://github.com/cube-js/cube/issues/10331
// Playground-scaffolded YAML models end with an empty `pre_aggregations:` key
// (see BaseSchemaFormatter: `pre_aggregations: new ValueWithComments(null, ...)`).
// Rollup Designer then fails with `'pre_aggregations' must be a sequence`.
describe('Issue #10331: Rollup Designer on scaffolded YAML model', () => {
  const scaffoldedModel = `cubes:
  - name: orders
    sql_table: public.orders
    data_source: default

    joins: []

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: created_at
        sql: created_at
        type: time

    measures:
      - name: count
        type: count

    pre_aggregations:
      # Pre-aggregation definitions go here.
      # Learn more in the documentation: https://docs.cube.dev/docs/pre-aggregations/getting-started-pre-aggregations
`;

  const code = `name: main
measures:
  - orders.count
time_dimension: orders.created_at
granularity: day
`;

  it('adds a pre-aggregation to a model with an empty pre_aggregations key', async () => {
    const repo = {
      localPath: () => __dirname,
      dataSchemaFiles: () => Promise.resolve([{ fileName: 'orders.yml', content: scaffoldedModel }]),
    };

    const schemaConverter = new CubeSchemaConverter(repo, [new CubePreAggregationConverter({
      cubeName: 'orders',
      preAggregationName: 'main',
      code,
    })]);

    await schemaConverter.generate('orders');

    const [file] = schemaConverter.getSourceFiles();
    const parsed = YAML.parse(file.source);
    expect(parsed.cubes[0].pre_aggregations).toEqual([
      {
        name: 'main',
        measures: ['orders.count'],
        time_dimension: 'orders.created_at',
        granularity: 'day',
      },
    ]);
  });
});
