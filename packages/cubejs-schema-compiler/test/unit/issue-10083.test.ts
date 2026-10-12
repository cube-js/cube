import { prepareYamlCompiler } from './PrepareCompiler';
import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';

// https://github.com/cube-js/cube/issues/10083
// BigQuery casts every `number` filter value to FLOAT64 (`org_id = CAST(? AS FLOAT64)`):
// BigqueryFilter.castParameter (legacy) and `tesseract.number_param_cast` (Tesseract) in
// src/adapter/BigqueryQuery.ts. Comparing an INT64/NUMERIC clustering column against FLOAT64
// coerces the column, which disables cluster pruning (and rounds integers above 2^53).
describe('Issue #10083: BigQuery number filters cast to FLOAT64', () => {
  const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(`
cubes:
  - name: fact
    sql_table: ds.fact
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: org_id
        sql: org_id
        type: number
    measures:
      - name: count
        type: count
`);

  beforeAll(async () => {
    await compiler.compile();
  });

  it('does not compare a number dimension against a FLOAT64 cast', () => {
    const query = new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['fact.count'],
      filters: [{ member: 'fact.org_id', operator: 'equals', values: ['4'] }],
      timezone: 'UTC',
    });

    const [sql] = query.buildSqlAndParams();

    expect(sql).toMatch(/org_id =/);
    expect(sql).not.toMatch(/AS FLOAT64/);
  });
});
