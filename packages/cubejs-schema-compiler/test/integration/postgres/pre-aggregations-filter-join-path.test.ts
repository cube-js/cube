import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';
import { testWithPreAggregation } from './pre-aggregation-utils';

// `sp_sales` reaches `brands` directly, or through `hcps` and the one_to_many
// `gross_sales`, which fans the sales out. The rollup stores the direct brand.
const SCHEMA = `
cubes:
  - name: sp_sales
    sql: >
      SELECT 1 AS id, 1 AS hcp_id, 1 AS brand_id, 10 AS amount UNION ALL
      SELECT 2, 1, 2, 20 UNION ALL
      SELECT 3, 2, 1, 40
    joins:
      - name: hcps
        relationship: many_to_one
        sql: "{CUBE}.hcp_id = {hcps}.id"
      - name: brands
        relationship: many_to_one
        sql: "{CUBE}.brand_id = {brands}.id"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: amount
        sql: amount
        type: sum
    pre_aggregations:
      - name: by_brand
        measures:
          - amount
        dimensions:
          - brands.name
      - name: by_gross_sale
        measures:
          - amount
        dimensions:
          - gross_sales.id

  - name: hcps
    sql: SELECT 1 AS id UNION ALL SELECT 2
    joins:
      - name: gross_sales
        relationship: one_to_many
        sql: "{CUBE}.id = {gross_sales}.hcp_id"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

  - name: gross_sales
    sql: SELECT 1 AS id, 1 AS hcp_id, 2 AS brand_id UNION ALL SELECT 2, 2, 1 UNION ALL SELECT 3, 2, 2
    joins:
      - name: brands
        relationship: many_to_one
        sql: "{CUBE}.brand_id = {brands}.id"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

  - name: brands
    sql: SELECT 1 AS id, 'A' AS name UNION ALL SELECT 2, 'B'
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: name
        sql: name
        type: string

views:
  - name: sales_by_gross_brand
    cubes:
      - join_path: sp_sales
        includes:
          - amount
      - join_path: sp_sales.hcps.gross_sales.brands
        includes:
          - name

  - name: sales_by_own_brand
    cubes:
      - join_path: sp_sales
        includes:
          - amount
      - join_path: sp_sales.brands
        includes:
          - name
`;

const PLANNERS: [string, boolean][] = [
  ['legacy planner', false],
  ['native planner', true],
];

describe('PreAggregations and the join path of filter members', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(SCHEMA);

  beforeAll(async () => {
    await compilers.compile();
  });

  const newQuery = (useNativeSqlPlanner: boolean, query: Record<string, unknown>) => compilers.createQuery(PostgresQuery, { ...query, timezone: 'UTC', preAggregationsSchema: '', useNativeSqlPlanner } as any);

  const usedTables = (query: any) => query.preAggregations?.preAggregationsDescription().map((d: any) => d.tableName);

  it.each(PLANNERS)('reads a filter over another path than the rollup from the source (%s)', async (_label, useNativeSqlPlanner) => {
    const query = newQuery(useNativeSqlPlanner, {
      measures: ['sales_by_gross_brand.amount'],
      filters: [{ member: 'sales_by_gross_brand.name', operator: 'equals', values: ['A'] }],
    });

    expect(usedTables(query)).toEqual([]);
    // Only hcp 2 has a gross sale of brand A; the rollup's path would give 50.
    expect(await dbRunner.testQuery(query.buildSqlAndParams())).toEqual([
      { sales_by_gross_brand__amount: '40' },
    ]);
  });

  it.each(PLANNERS)('agrees with the same member as a dimension (%s)', async (_label, useNativeSqlPlanner) => {
    const query = newQuery(useNativeSqlPlanner, {
      measures: ['sales_by_gross_brand.amount'],
      dimensions: ['sales_by_gross_brand.name'],
      order: [{ id: 'sales_by_gross_brand.name', desc: false }],
    });

    expect(usedTables(query)).toEqual([]);
    expect(await dbRunner.testQuery(query.buildSqlAndParams())).toEqual([
      { sales_by_gross_brand__name: 'A', sales_by_gross_brand__amount: '40' },
      { sales_by_gross_brand__name: 'B', sales_by_gross_brand__amount: '70' },
    ]);
  });

  it.each(PLANNERS)('serves a filter over the rollup path from the rollup (%s)', async (_label, useNativeSqlPlanner) => {
    const query = newQuery(useNativeSqlPlanner, {
      measures: ['sales_by_own_brand.amount'],
      filters: [{ member: 'sales_by_own_brand.name', operator: 'equals', values: ['A'] }],
    });

    expect(usedTables(query)).toEqual(['sp_sales_by_brand']);
    const description: any = query.preAggregations?.preAggregationsDescription();
    expect(await testWithPreAggregation(description[0], query)).toEqual([
      { sales_by_own_brand__amount: '50' },
    ]);
  });

  // Hcp 2 has gross sales 2 and 3, so `by_gross_sale` stores its sale under both.
  it.each(PLANNERS)('does not roll a multiplied rollup up over a filter member (%s)', async (_label, useNativeSqlPlanner) => {
    const query = newQuery(useNativeSqlPlanner, {
      measures: ['sp_sales.amount'],
      filters: [{ member: 'gross_sales.id', operator: 'equals', values: ['2', '3'] }],
    });

    expect(usedTables(query)).toEqual([]);
    expect(await dbRunner.testQuery(query.buildSqlAndParams())).toEqual([
      { sp_sales__amount: '40' },
    ]);
  });
});
