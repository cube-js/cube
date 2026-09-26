// Repro for https://github.com/cube-js/cube/issues/10331
// "Rollup Designer doesn't work: 'pre_aggregations' must be a sequence"
//
// Rollup Designer (Playground) calls DevServer POST /playground/schema/pre-aggregation, which runs
// CubeSchemaConverter + CubePreAggregationConverter over the data model files. YAML models produced by
// Cube's own scaffolding (BaseSchemaFormatter / "Generate Data Model" in Playground) contain an
// empty `pre_aggregations:` key followed only by comments, i.e. a null value. The converter rejects it.
import YAML from 'yaml';
import { CubePreAggregationConverter, CubeSchemaConverter } from '../../src';

const scaffoldedYaml = `cubes:
  - name: orders
    sql_table: public.orders

    joins: []

    dimensions:
      - name: status
        sql: status
        type: string

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

const repo = (content: string) => ({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'orders.yml', content }]),
});

// What Rollup Designer sends when "YAML" format is selected (js-yaml dump of the definition)
const yamlCode = `name: main
measures:
  - orders.count
dimensions:
  - orders.status
timeDimension: orders.created_at
granularity: year
`;

// What Rollup Designer sends by default (JS format is the default in the OSS Playground)
const jsCode = `{
  measures: [
    orders.count
  ],
  dimensions: [
    orders.status
  ],
  timeDimension: orders.created_at,
  granularity: \`year\`
}`;

function preAggsOf(source: string): any[] {
  const doc = YAML.parse(source);
  return doc.cubes[0].pre_aggregations;
}

describe('issue #10331: Rollup Designer adding pre-aggregation to YAML model', () => {
  it('adds a pre-aggregation to a scaffolded YAML model with an empty `pre_aggregations:` key', async () => {
    const converter = new CubeSchemaConverter(repo(scaffoldedYaml), [
      new CubePreAggregationConverter({ cubeName: 'orders', preAggregationName: 'main', code: yamlCode }),
    ]);

    // Currently throws UserError: 'pre_aggregations' must be a sequence
    await converter.generate('orders');

    const [file] = converter.getSourceFiles();
    const preAggs = preAggsOf(file.source);
    expect(preAggs).toHaveLength(1);
    expect(preAggs[0].name).toBe('main');
  });

  it('does not write a nameless (invalid) pre-aggregation into a YAML model when JS-format code is sent', async () => {
    const withoutPreAggsKey = scaffoldedYaml.replace(/\n {4}pre_aggregations:[\s\S]*$/, '\n');
    const converter = new CubeSchemaConverter(repo(withoutPreAggsKey), [
      new CubePreAggregationConverter({ cubeName: 'orders', preAggregationName: 'main', code: jsCode }),
    ]);

    let source: string | null = null;

    try {
      await converter.generate('orders');
      [{ source }] = converter.getSourceFiles();
    } catch {
      // Rejecting mismatched format is acceptable too
      return;
    }

    // Currently succeeds and writes `- { measures: [...], ... }` with no `name`, which then fails to
    // compile with "name isn't defined for preAggregation".
    const preAggs = preAggsOf(source!);
    expect(preAggs[0].name).toBe('main');
  });
});
