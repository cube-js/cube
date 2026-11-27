import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// Measures from two fact cubes that share a dimension are aggregated separately, and the
// aggregates are then joined on that dimension with a null-safe comparison. BigQuery does
// not plan `IS NOT DISTINCT FROM` as a hash join, so it has to use the
// `a = b OR (a IS NULL AND b IS NULL)` form instead.
const model = `
cubes:
  - name: accounts
    sql: SELECT 1 AS id, 'acme' AS name
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: name
        sql: "{CUBE}.name"
        type: string

  - name: orders
    sql: SELECT 1 AS id, 1 AS account_id, 10 AS amount
    joins:
      - name: accounts
        sql: "{CUBE}.account_id = {accounts}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
    measures:
      - name: total_amount
        sql: "{CUBE}.amount"
        type: sum

  - name: refunds
    sql: SELECT 1 AS id, 1 AS account_id, 2 AS amount
    joins:
      - name: accounts
        sql: "{CUBE}.account_id = {accounts}.id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
    measures:
      - name: total_amount
        sql: "{CUBE}.amount"
        type: sum
`;

describe('Null-safe join of per-fact aggregates', () => {
  let compilers: any;

  beforeAll(async () => {
    compilers = prepareYamlCompiler(model);
    await compilers.compile();
  });

  it('joins each aggregate to the key set with equality-or-both-null on BigQuery', () => {
    const [sql] = compilers.createQuery(BigqueryQuery, {
      measures: ['orders.total_amount', 'refunds.total_amount'],
      dimensions: ['accounts.name'],
      timezone: 'UTC',
      useNativeSqlPlanner: true,
    }).buildSqlAndParams();

    expect(sql).not.toContain('IS NOT DISTINCT FROM');

    for (const alias of ['q_0', 'q_1']) {
      const key = '`fk_aggregate_keys`.`accounts__name`';
      const aggregateKey = `\`${alias}\`.\`accounts__name\``;

      expect(sql).toContain(
        `((${key} = ${aggregateKey} OR ((${key} IS NULL) AND (${aggregateKey} IS NULL))))`
      );
    }
  });
});
