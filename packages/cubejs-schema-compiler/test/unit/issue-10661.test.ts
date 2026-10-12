import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// #10661: a multi-stage `case` measure switching on a `type: switch` dimension.
// When the switch dimension is neither selected nor filtered, the measure must
// resolve to its `else` branch (amount_usd), giving one row per query
// dimension value. Instead Tesseract expands every switch value into its own
// leaf CTE, keys the result by the switch dimension, and emits a CASE without
// ELSE, so the query returns one row per (status, currency) with mixed
// currencies. (With rows (1,a,usd=10,eur=9,nok=100),(2,b,20,18,200), executing
// the generated SQL in DuckDB returned 6 rows:
// a=100/10/9, b=200/20/18; expected a=10, b=20.) The legacy planner is also
// wrong (it returns orders.id as dynamic_amount).
// An integration test executing this against Postgres is proposed in PR #12180.
describe('Issue #10661 - switch dimension + multi-stage case measure without the dimension', () => {
  const model = `
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
      - name: currency_selector
        type: switch
        values:
          - USD
          - EUR
          - NOK
    measures:
      - name: amount_usd
        sql: amount_usd
        type: sum
      - name: amount_eur
        sql: amount_eur
        type: sum
      - name: amount_nok
        sql: amount_nok
        type: sum
      - name: dynamic_amount
        type: number
        multi_stage: true
        case:
          switch: "{CUBE.currency_selector}"
          when:
            - value: EUR
              sql: "{CUBE.amount_eur}"
            - value: NOK
              sql: "{CUBE.amount_nok}"
            - value: USD
              sql: "{CUBE.amount_usd}"
          else:
            sql: "{CUBE.amount_usd}"
`;

  it('uses only the else branch when the switch dimension is not in the query', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['orders.dynamic_amount'],
      dimensions: ['orders.status'],
      timezone: 'UTC',
    });
    const [sql] = query.buildSqlAndParams();

    // Only the else branch (amount_usd) applies; the other currencies' measures
    // must not be computed and mixed into the result.
    expect(sql).toContain('amount_usd');
    expect(sql).not.toMatch(/amount_eur/);
    expect(sql).not.toMatch(/amount_nok/);
  });
});
