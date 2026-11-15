import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// A dimension-only request whose only member of the root cube is a filter:
// brand.squad filtered by profile.id. The query is planned from profile, through
// a one_to_many hop (profile -> hcp x brand lookup -> brand master), and the
// rollup stores the same columns over the same path. Tesseract up to v1.8.25
// built the query's join groups without its filters, rooted them at brand, and
// refused the rollup on a join-path mismatch.
describe('Dimension-only query through a one_to_many hop matches a rollup', () => {
  const compilers = prepareYamlCompiler(`
cubes:
  - name: profile
    sql_table: public.profile
    joins:
      - name: account
        sql: "{CUBE.site_id} = {account.id}"
        relationship: many_to_one
      - name: brand_lookup
        sql: "{CUBE.id} = {brand_lookup.hcp_id}"
        relationship: one_to_many
      - name: sales
        sql: "{CUBE.id} = {sales.hcp_id}"
        relationship: one_to_many
    dimensions:
      - name: id
        sql: id
        type: string
        primary_key: true
      - name: site_id
        sql: site_id
        type: string
    measures:
      - name: count
        sql: "1"
        type: count
    pre_aggregations:
      - name: hcp_brand_site_filters
        measures:
          - count
        dimensions:
          - id
          - profile.site_id
          - profile.account.city
          - brand_lookup.brand.brand_name
          - brand_lookup.brand.squad

  - name: account
    sql_table: public.account
    dimensions:
      - name: id
        sql: id
        type: string
        primary_key: true
      - name: city
        sql: city
        type: string

  - name: brand_lookup
    sql: SELECT DISTINCT hcp_id, brand_name FROM public.sales
    joins:
      - name: brand
        sql: "{CUBE.brand_name} = {brand.brand_name}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "hcp_id || '|' || brand_name"
        type: string
        primary_key: true
      - name: hcp_id
        sql: hcp_id
        type: string
      - name: brand_name
        sql: brand_name
        type: string

  - name: sales
    sql_table: public.sales
    joins:
      - name: brand
        sql: "{CUBE.brand_name} = {brand.brand_name}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: string
        primary_key: true
      - name: hcp_id
        sql: hcp_id
        type: string
      - name: brand_name
        sql: brand_name
        type: string
    measures:
      - name: gross_sales
        sql: amount
        type: sum

  - name: brand
    sql_table: public.brand
    dimensions:
      - name: brand_id
        sql: brand_id
        type: string
        primary_key: true
      - name: brand_name
        sql: brand_name
        type: string
      - name: squad
        sql: squad
        type: string

views:
  - name: perf_view
    cubes:
      - join_path: profile
        includes:
          - id
          - site_id
      - join_path: sales
        includes:
          - gross_sales
      - join_path: profile.account
        includes:
          - city
      - join_path: brand
        includes:
          - squad
          - brand_name
`);

  beforeAll(async () => {
    await compilers.compile();
  });

  const run = (query: Record<string, any>) => {
    const pgQuery = compilers.createQuery(PostgresQuery, {
      ...query,
      timezone: 'UTC',
      preAggregationsSchema: '',
    });
    const preAggregations: any[] = pgQuery.preAggregations?.preAggregationsDescription() || [];
    return { tableNames: preAggregations.map(p => p.tableName) };
  };

  it('view: squad filtered by id', () => {
    const { tableNames } = run({
      dimensions: ['perf_view.squad'],
      filters: [{ member: 'perf_view.id', operator: 'equals', values: ['1'] }],
      order: [{ id: 'perf_view.squad', desc: false }],
      limit: 10,
    });
    expect(tableNames).toEqual(['profile_hcp_brand_site_filters']);
  });

  it('cube: squad filtered by id', () => {
    const { tableNames } = run({
      dimensions: ['brand.squad'],
      filters: [{ member: 'profile.id', operator: 'equals', values: ['1'] }],
    });
    expect(tableNames).toEqual(['profile_hcp_brand_site_filters']);
  });

  it('cube with join path: squad filtered by id', () => {
    const { tableNames } = run({
      dimensions: ['profile.brand_lookup.brand.squad'],
      filters: [{ member: 'profile.id', operator: 'equals', values: ['1'] }],
    });
    expect(tableNames).toEqual(['profile_hcp_brand_site_filters']);
  });
});
