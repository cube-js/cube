// Reproduction for https://github.com/cube-js/cube/issues/10331
// Rollup Designer fails with "'pre_aggregations' must be a sequence" on YAML models.
import YAML from 'yaml';
import { CubePreAggregationConverter, CubeSchemaConverter } from '../../src';

// This is exactly what the Playground "Generate Data Model" (YAML) scaffolding emits:
// an empty `pre_aggregations:` key followed by comments (value is null).
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

const repoWith = (content: string) => ({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'orders.yml', content }]),
});

const addPreAgg = async (content: string, code: string) => {
  const converter = new CubeSchemaConverter(repoWith(content), [
    new CubePreAggregationConverter({ cubeName: 'orders', preAggregationName: 'main', code }),
  ]);
  await converter.generate('orders');
  const file = converter.getSourceFiles().find(({ cubeName }) => cubeName === 'orders');
  return YAML.parse(file!.source);
};

// YAML code as sent by RollupDesigner when the YAML tab is selected
const yamlCode = 'name: main\nmeasures:\n  - orders.count\ntimeDimension: orders.created_at\ngranularity: day\n';

describe('CubePreAggregationConverter (#10331)', () => {
  it('adds a pre-aggregation to a scaffolded YAML model with an empty `pre_aggregations:` key', async () => {
    const parsed = await addPreAgg(scaffoldedModel, yamlCode);
    expect(parsed.cubes[0].pre_aggregations).toEqual([
      { name: 'main', measures: ['orders.count'], timeDimension: 'orders.created_at', granularity: 'day' },
    ]);
  });

  it('adds a pre-aggregation to a YAML model with `pre_aggregations: []`', async () => {
    const parsed = await addPreAgg(scaffoldedModel.replace(/pre_aggregations:[\s\S]*$/, 'pre_aggregations: []\n'), yamlCode);
    expect(parsed.cubes[0].pre_aggregations).toHaveLength(1);
  });
});
