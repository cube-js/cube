import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/10083
// BigQuery casts every `number` filter value to FLOAT64: `org_id = 4` is rendered as
// `org_id = CAST(? AS FLOAT64)`. Comparing an INT64/NUMERIC clustering column against a
// FLOAT64 stops BigQuery from pruning clustered blocks, so a filtered query bills the full
// table scan. FLOAT64 is not a type BigQuery can cluster on, and it also rounds integers
// above 2^53. Reproduced against the generated SQL of a running Cube v1.7.48.
describe('Issue #10083: BigQuery number filters cast to FLOAT64', () => {
  const compilers = prepareYamlCompiler(`
cubes:
  - name: fact
    sql: "SELECT 1 AS id, 4 AS org_id, 108 AS time_period_id"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: org_id
        sql: org_id
        type: number
      - name: time_period_id
        sql: time_period_id
        type: number
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  it.each([false, true])('does not compare number dimensions to a FLOAT64 (Tesseract: %s)', (useNativeSqlPlanner) => {
    const query = new BigqueryQuery(compilers, {
      measures: ['fact.count'],
      filters: [
        { member: 'fact.org_id', operator: 'equals', values: ['4'] },
        { member: 'fact.time_period_id', operator: 'equals', values: ['108'] },
      ],
      timezone: 'UTC',
      useNativeSqlPlanner,
    });

    const [sql] = query.buildSqlAndParams();

    expect(sql).toMatch(/org_id/);
    expect(sql).not.toMatch(/AS FLOAT64/);
  });
});
