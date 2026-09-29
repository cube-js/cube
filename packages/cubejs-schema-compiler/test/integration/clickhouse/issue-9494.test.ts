import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// https://github.com/cube-js/cube/issues/9494
// ClickHouse returns UInt8 (0/1) for comparisons, so a `boolean` dimension or a segment
// written as a comparison lands in the rollup as an int column. Cube Store then rejects
// the segment as a filter predicate ("non-boolean predicate ... returning Int64") and the
// SQL API cannot map the column back to a boolean ("Unable to map value Number(0.0) to
// Boolean"). The rollup must hold real booleans, as it does when built on Postgres.
describe('ClickHouse issue 9494: booleans in pre-aggregations', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('top_card_metrics', {
      sql: \`
        SELECT '17390' AS brand_id, 'blinkit' AS brand, 2 AS offtake_mrp
        UNION ALL
        SELECT '123' AS brand_id, 'foo' AS brand, 4 AS offtake_mrp
      \`,
      dimensions: {
        brand_id: { sql: 'brand_id', type: 'string' },
        brand: { sql: 'brand', type: 'string' },
        is_current_brand: { sql: \`brand_id = '17390'\`, type: 'boolean' },
      },
      measures: {
        offtake_mrp_sum: { sql: 'offtake_mrp', type: 'sum' },
      },
      segments: {
        curr_brand: { sql: \`\${CUBE}.brand_id = '17390'\` },
      },
      pre_aggregations: {
        main: {
          type: 'rollup',
          measures: [CUBE.offtake_mrp_sum],
          dimensions: [CUBE.brand_id, CUBE.brand, CUBE.is_current_brand],
          segments: [CUBE.curr_brand],
          indexes: { brand_index: { columns: [CUBE.brand_id] } },
        },
      },
    });
  `);

  beforeAll(async () => {
    await compiler.compile();
  });

  // The SELECT the rollup is built from, i.e. the loadSql without its CREATE TABLE prefix
  const rollupBuildRows = async () => {
    const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['top_card_metrics.offtake_mrp_sum'],
      dimensions: ['top_card_metrics.brand', 'top_card_metrics.is_current_brand'],
      segments: ['top_card_metrics.curr_brand'],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });
    const description: any = query.preAggregations?.preAggregationsDescription();
    expect(description.length).toEqual(1);

    const [loadSql, params] = description[0].loadSql;
    const match = /\sAS\s+(SELECT[\s\S]*)$/i.exec(loadSql);
    expect(match).not.toBeNull();

    const rows = await dbRunner.testQuery([match![1], params], noDataSet);
    return rows.sort((a, b) => String(a.top_card_metrics__brand).localeCompare(String(b.top_card_metrics__brand)));
  };

  it('stores a boolean dimension as a boolean in the rollup', async () => {
    const rows = await rollupBuildRows();

    expect(rows.map(r => [r.top_card_metrics__brand, r.top_card_metrics__is_current_brand])).toEqual([
      ['blinkit', true],
      ['foo', false],
    ]);
  });

  it('stores a segment as a boolean in the rollup', async () => {
    const rows = await rollupBuildRows();

    expect(rows.map(r => [r.top_card_metrics__brand, r.top_card_metrics__curr_brand])).toEqual([
      ['blinkit', true],
      ['foo', false],
    ]);
  });
});
