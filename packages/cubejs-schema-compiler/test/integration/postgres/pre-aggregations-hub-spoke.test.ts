import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';
import { testWithPreAggregation } from './pre-aggregation-utils';

// Joins are declared only from `hub` and `ledger`, so `entities` is reachable
// only through `hub`: the members of `ledger`, `categories` and `entities` have
// no join root on their own.
const CUBES = `
cubes:
  - name: hub
    sql: >
      SELECT 1 AS id, 'o1' AS org_id, 'e1' AS entity_id, 'm1' AS management_id UNION ALL
      SELECT 2, 'o1', 'e2', 'm2' UNION ALL
      SELECT 3, 'o2', 'e1', 'm3'
    joins:
      - name: ledger
        relationship: one_to_many
        sql: "{CUBE.management_id} = {ledger.management_id}"
      - name: entities
        relationship: many_to_one
        sql: "{CUBE.entity_id} = {entities.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: org_id
        sql: org_id
        type: string
      - name: entity_id
        sql: entity_id
        type: string
      - name: management_id
        sql: management_id
        type: string
    measures:
      - name: count
        type: count
    pre_aggregations:
`;

const SPOKES = `
  - name: ledger
    sql: >
      SELECT 1 AS id, 'm1' AS management_id, 'c1' AS category_id, 10 AS amount UNION ALL
      SELECT 2, 'm1', 'c2', 20 UNION ALL
      SELECT 3, 'm2', 'c1', 30 UNION ALL
      SELECT 4, 'm3', 'c1', 40 UNION ALL
      SELECT 5, 'm3', 'c2', 50
    joins:
      - name: categories
        relationship: many_to_one
        sql: "{CUBE.category_id} = {categories.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: management_id
        sql: management_id
        type: string
      - name: category_id
        sql: category_id
        type: string
    measures:
      - name: amount
        sql: amount
        type: sum

  - name: categories
    sql: >
      SELECT 'c1' AS id, 'Fees' AS name UNION ALL
      SELECT 'c2', 'Rent'
    dimensions:
      - name: id
        sql: id
        type: string
        primary_key: true
      - name: name
        sql: name
        type: string

  - name: entities
    sql: >
      SELECT 'e1' AS id, 'Entity A' AS name UNION ALL
      SELECT 'e2', 'Entity B'
    dimensions:
      - name: id
        sql: id
        type: string
        primary_key: true
      - name: name
        sql: name
        type: string
`;

// The only hub member is a measure.
const INCOME_BY_CATEGORY = `
      - name: income_by_category
        measures:
          - count
          - ledger.amount
        dimensions:
          - ledger.category_id
          - categories.name
          - entities.name
`;

// Kept apart from `income_by_category`, so a query only reaches it through matching.
const INCOME_BY_ORG = `
      - name: income_by_org
        measures:
          - ledger.amount
        dimensions:
          - org_id
          - categories.name
          - entities.name
`;

const PLANNERS: [string, boolean][] = [
  ['legacy planner', false],
  ['native planner', true],
];

describe('PreAggregations over cubes joined only through a hub', () => {
  jest.setTimeout(200000);

  const raw = prepareYamlCompiler(CUBES + SPOKES);

  const newQuery = (compilers: any, useNativeSqlPlanner: boolean, query: Record<string, unknown>) => new PostgresQuery(
    compilers,
    { ...query, timezone: 'UTC', preAggregationsSchema: '', useNativeSqlPlanner } as any
  );

  const readThroughPreAggregation = async (
    preAggregations: string,
    useNativeSqlPlanner: boolean,
    query: Record<string, unknown>,
    tableName: string,
  ) => {
    const withPreAggregations = prepareYamlCompiler(CUBES + preAggregations + SPOKES);
    await withPreAggregations.compiler.compile();
    await raw.compiler.compile();

    const preAggQuery = newQuery(withPreAggregations, useNativeSqlPlanner, query);
    const description: any = preAggQuery.preAggregations?.preAggregationsDescription();
    expect(description.map((d: any) => d.tableName)).toEqual([tableName]);
    expect(preAggQuery.buildSqlAndParams()[0]).toContain(tableName);

    const fromPreAggregation = await testWithPreAggregation(description[0], preAggQuery);
    const fromRawTables = await dbRunner.testQuery(
      newQuery(raw, useNativeSqlPlanner, query).buildSqlAndParams()
    );
    expect(fromPreAggregation).toEqual(fromRawTables);
    return fromPreAggregation;
  };

  it.each(PLANNERS)('serves a rollup whose only hub member is a measure (%s)', async (_label, useNativeSqlPlanner) => {
    const res = await readThroughPreAggregation(INCOME_BY_CATEGORY, useNativeSqlPlanner, {
      measures: ['hub.count', 'ledger.amount'],
      dimensions: ['ledger.category_id', 'categories.name', 'entities.name'],
      order: [{ id: 'ledger.category_id', desc: false }, { id: 'entities.name', desc: false }],
    }, 'hub_income_by_category');

    expect(res).toEqual([
      {
        ledger__category_id: 'c1',
        categories__name: 'Fees',
        entities__name: 'Entity A',
        hub__count: '2',
        ledger__amount: '50',
      },
      {
        ledger__category_id: 'c1',
        categories__name: 'Fees',
        entities__name: 'Entity B',
        hub__count: '1',
        ledger__amount: '30',
      },
      {
        ledger__category_id: 'c2',
        categories__name: 'Rent',
        entities__name: 'Entity A',
        hub__count: '2',
        ledger__amount: '70',
      },
    ]);
  });

  it.each(PLANNERS)('matches a rollup when the hub is only in a filter (%s)', async (_label, useNativeSqlPlanner) => {
    const res = await readThroughPreAggregation(INCOME_BY_ORG, useNativeSqlPlanner, {
      measures: ['ledger.amount'],
      dimensions: ['categories.name', 'entities.name'],
      filters: [{ member: 'hub.org_id', operator: 'equals', values: ['o1'] }],
      order: [{ id: 'categories.name', desc: false }, { id: 'entities.name', desc: false }],
    }, 'hub_income_by_org');

    expect(res).toEqual([
      { categories__name: 'Fees', entities__name: 'Entity A', ledger__amount: '10' },
      { categories__name: 'Fees', entities__name: 'Entity B', ledger__amount: '30' },
      { categories__name: 'Rent', entities__name: 'Entity A', ledger__amount: '20' },
    ]);
  });
});
