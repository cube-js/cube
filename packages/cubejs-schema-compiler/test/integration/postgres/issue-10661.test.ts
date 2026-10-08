import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/10661
describe('Issue 10661: case measure over a switch dimension that is not in the query', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT 1 AS id, 'a' AS status, 10.0 AS amount_usd, 9.0 AS amount_eur, 100.0 AS amount_nok
      UNION ALL
      SELECT 2 AS id, 'b' AS status, 20.0 AS amount_usd, 18.0 AS amount_eur, 200.0 AS amount_nok

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
          switch: "{currency_selector}"
          when:
            - value: EUR
              sql: "{amount_eur}"
            - value: NOK
              sql: "{amount_nok}"
            - value: USD
              sql: "{amount_usd}"
          else:
            sql: "{amount_usd}"
`);

  // Control: with the switch dimension filtered, a single row in that currency.
  it('filtered by currency_selector', async () => dbRunner.runQueryTest({
    measures: ['orders.dynamic_amount'],
    filters: [{ member: 'orders.currency_selector', operator: 'equals', values: ['EUR'] }],
  }, [
    { orders__dynamic_amount: '27.0' },
  ], { joinGraph, cubeEvaluator, compiler }));

  it('without currency_selector: one row in the else (USD) currency', async () => dbRunner.runQueryTest({
    measures: ['orders.dynamic_amount'],
  }, [
    { orders__dynamic_amount: '30.0' },
  ], { joinGraph, cubeEvaluator, compiler }));

  it('without currency_selector, grouped by another dimension', async () => dbRunner.runQueryTest({
    measures: ['orders.dynamic_amount'],
    dimensions: ['orders.status'],
    order: [{ id: 'orders.status' }],
  }, [
    { orders__status: 'a', orders__dynamic_amount: '10.0' },
    { orders__status: 'b', orders__dynamic_amount: '20.0' },
  ], { joinGraph, cubeEvaluator, compiler }));
});
