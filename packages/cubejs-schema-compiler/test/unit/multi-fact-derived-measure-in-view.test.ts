import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// AOV ("basket") as a retailer models it: the numerator and the denominator sit
// in two different fact tables at two different grains.
//
//   sales_line_item     - one row per transaction line
//   item_location_sales - one row per day/item/location
//
// `transactions_without_returns` is a count distinct of transaction ids on the
// line-item cube, narrowed by filters that belong to that cube (transaction
// type, fulfillment channel group). Those filters are written once, on the cube
// that owns the columns, and every consumer picks them up by including the
// measure - they are never restated in a view. `sales_amount` is a plain sum on
// the day/item/location cube.
//
// The ratio of the two is authored as a measure of the view rather than per
// consumer. The line-item side has to be aggregated to the query grain before
// it can divide a sum coming from the other fact table, which is the multi-fact
// path: both facts join to the shared `items`, `locations` and `dates` cubes,
// but never to each other.
const model = `
cubes:
  - name: items
    sql: >
      SELECT 1 AS id, 'Bakery' AS department UNION ALL
      SELECT 2 AS id, 'Produce' AS department
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: department
        sql: "{CUBE}.department"
        type: string

  - name: locations
    sql: >
      SELECT 1 AS id, 'West' AS region UNION ALL
      SELECT 2 AS id, 'East' AS region
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: region
        sql: "{CUBE}.region"
        type: string

  # Date spine shared by both facts. Without it the two facts have no common
  # time member to stitch on: one is keyed by day, the other by timestamp.
  - name: dates
    sql: >
      SELECT '2026-01-01'::timestamp AS date UNION ALL
      SELECT '2026-01-02'::timestamp AS date
    dimensions:
      - name: date
        sql: "{CUBE}.date"
        type: time
        primary_key: true

  - name: sales_line_item
    sql: >
      SELECT 1 AS id, 100 AS transaction_id, 1 AS item_id, 1 AS location_id,
             'SALE' AS transaction_type, 'IN_STORE' AS fulfillment_channel_group,
             '2026-01-01'::timestamp AS sold_at
    joins:
      - name: items
        sql: "{CUBE}.item_id = {items}.id"
        relationship: many_to_one
      - name: locations
        sql: "{CUBE}.location_id = {locations}.id"
        relationship: many_to_one
      - name: dates
        sql: "DATE_TRUNC('day', {CUBE}.sold_at) = {dates.date}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: transaction_id
        sql: "{CUBE}.transaction_id"
        type: number
      - name: transaction_type
        sql: "{CUBE}.transaction_type"
        type: string
      - name: fulfillment_channel_group
        sql: "{CUBE}.fulfillment_channel_group"
        type: string
      - name: sold_at
        sql: "{CUBE}.sold_at"
        type: time
    segments:
      - name: net_sale_transactions
        sql: "{CUBE}.transaction_type NOT IN ('RETURN', 'EXCHANGE')"
    measures:
      - name: transactions_without_returns
        sql: "{CUBE}.transaction_id"
        type: count_distinct
        filters:
          - sql: "{CUBE}.transaction_type <> 'EXCHANGE'"
          - sql: "{CUBE}.fulfillment_channel_group IN ('IN_STORE', 'SHIP_FROM_STORE')"

  - name: item_location_sales
    sql: >
      SELECT 1 AS id, 1 AS item_id, 1 AS location_id,
             '2026-01-01'::timestamp AS date, 30 AS sales_amount
    joins:
      - name: items
        sql: "{CUBE}.item_id = {items}.id"
        relationship: many_to_one
      - name: locations
        sql: "{CUBE}.location_id = {locations}.id"
        relationship: many_to_one
      - name: dates
        sql: "DATE_TRUNC('day', {CUBE}.date) = {dates.date}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: date
        sql: "{CUBE}.date"
        type: time
    measures:
      - name: sales_amount
        sql: "{CUBE}.sales_amount"
        type: sum

views:
  - name: retail_analysis
    cubes:
      - join_path: item_location_sales
        includes:
          - sales_amount
      - join_path: sales_line_item
        includes:
          - transactions_without_returns
          - net_sale_transactions
      # The shared dimension cubes sit at root-level join paths so their
      # dimensions are common to both facts.
      - join_path: dates
        includes:
          - date
      - join_path: items
        includes:
          - department
      - join_path: locations
        includes:
          - region
    measures:
      # References inside a view measure are resolved against the view, so they
      # have to be written as \`{view.member}\`; a bare \`{member}\` does not
      # resolve. \`multi_stage\` is what lets the ratio be evaluated after both
      # facts have been aggregated - see the tests below.
      - name: aov_basket
        type: number
        multi_stage: true
        sql: "{retail_analysis.sales_amount} / NULLIF({retail_analysis.transactions_without_returns}, 0)"
      # Same expression without \`multi_stage\`, kept to pin what happens when
      # the ratio is planned as an ordinary calculated measure.
      - name: aov_basket_single_stage
        type: number
        sql: "{retail_analysis.sales_amount} / NULLIF({retail_analysis.transactions_without_returns}, 0)"
`;

const buildSql = async (query: any, useNativeSqlPlanner: boolean = true) => {
  const compilers = prepareYamlCompiler(model);
  await compilers.compiler.compile();

  const [sql] = new PostgresQuery(compilers, {
    timezone: 'UTC',
    useNativeSqlPlanner,
    ...query,
  }).buildSqlAndParams();

  return sql;
};

// Both facts, aggregated on their own before anything is combined.
const SALES_AMOUNT_AGGREGATE = /sum\("item_location_sales"\.sales_amount\)/;
const TRANSACTIONS_AGGREGATE = /COUNT\(DISTINCT CASE WHEN .* THEN "sales_line_item"\.transaction_id END\)/;
// The ratio, taken over the two aggregates once they are lined up on the
// query's dimensions.
const RATIO_OVER_AGGREGATES =
  /"q_0"\."item_location_sales__sales_amount" \/ NULLIF\("q_1"\."sales_line_item__transactions_without_returns", 0\)/;

// Multi-fact queries are planned by Tesseract only, so everything that is
// expected to produce SQL runs against the native planner.
describe('Multi-fact derived measure defined on a view', () => {
  it('aggregates each fact cube separately when the components are queried side by side', async () => {
    const sql = await buildSql({
      measures: [
        'retail_analysis.sales_amount',
        'retail_analysis.transactions_without_returns',
      ],
      dimensions: ['retail_analysis.region'],
    });

    expect(sql).toMatch(SALES_AMOUNT_AGGREGATE);
    expect(sql).toMatch(TRANSACTIONS_AGGREGATE);
    // The line-item filters travel with the measure - the view does not restate
    // them.
    expect(sql).toContain('"sales_line_item".transaction_type <> \'EXCHANGE\'');
    expect(sql).toContain('"sales_line_item".fulfillment_channel_group IN (\'IN_STORE\', \'SHIP_FROM_STORE\')');
    // Each fact reaches the shared dimension through its own join.
    expect(sql).toContain('"item_location_sales".location_id = "locations".id');
    expect(sql).toContain('"sales_line_item".location_id = "locations".id');
  });

  it('divides the two facts once both have been aggregated to the query grain', async () => {
    const sql = await buildSql({
      measures: ['retail_analysis.aov_basket'],
      dimensions: ['retail_analysis.region'],
    });

    expect(sql).toMatch(SALES_AMOUNT_AGGREGATE);
    expect(sql).toMatch(TRANSACTIONS_AGGREGATE);
    expect(sql).toMatch(RATIO_OVER_AGGREGATES);
    // The division is not pushed into either fact's own aggregation.
    expect(sql).not.toMatch(/sum\("item_location_sales"\.sales_amount\) \/ NULLIF/);
  });

  it('divides the two facts on the shared date spine', async () => {
    const sql = await buildSql({
      measures: ['retail_analysis.aov_basket'],
      timeDimensions: [{ dimension: 'retail_analysis.date', granularity: 'day' }],
    });

    expect(sql).toContain('DATE_TRUNC(\'day\', "item_location_sales".date) = "dates".date');
    expect(sql).toContain('DATE_TRUNC(\'day\', "sales_line_item".sold_at) = "dates".date');
    expect(sql).toMatch(RATIO_OVER_AGGREGATES);
  });

  it('returns the ratio next to its components', async () => {
    const sql = await buildSql({
      measures: [
        'retail_analysis.sales_amount',
        'retail_analysis.transactions_without_returns',
        'retail_analysis.aov_basket',
      ],
      dimensions: ['retail_analysis.department'],
    });

    expect(sql).toMatch(SALES_AMOUNT_AGGREGATE);
    expect(sql).toMatch(TRANSACTIONS_AGGREGATE);
    expect(sql).toMatch(RATIO_OVER_AGGREGATES);
  });

  it('filters the ratio by a dimension shared between the facts', async () => {
    const sql = await buildSql({
      measures: ['retail_analysis.aov_basket'],
      dimensions: ['retail_analysis.region'],
      filters: [{
        member: 'retail_analysis.department',
        operator: 'equals',
        values: ['Bakery'],
      }],
    });

    expect(sql).toMatch(RATIO_OVER_AGGREGATES);
    // Both facts are narrowed, each through its own join to `items`.
    expect(sql).toContain('"item_location_sales".item_id = "items".id');
    expect(sql).toContain('"sales_line_item".item_id = "items".id');
  });

  // Current behaviour, pinned. Without `multi_stage` the ratio is planned as an
  // ordinary calculated measure, so the planner looks for a single join tree
  // covering both fact cubes and there is none - the two facts only meet
  // through the shared dimensions.
  it('cannot plan the ratio when the view measure is not multi_stage', async () => {
    await expect(buildSql({
      measures: ['retail_analysis.aov_basket_single_stage'],
      dimensions: ['retail_analysis.region'],
    })).rejects.toThrow(/Can't find join path to join .*item_location_sales.*sales_line_item/);
  });

  // Current behaviour, pinned. A segment is the other place cube-owned filter
  // logic could be written once and reused; it does not survive the multi-fact
  // split, so shared filter logic has to live in the measure's own `filters:`
  // (which does travel - see the first test).
  it('cannot plan a multi-fact query that carries a segment', async () => {
    await expect(buildSql({
      measures: ['retail_analysis.aov_basket'],
      dimensions: ['retail_analysis.region'],
      segments: ['retail_analysis.net_sale_transactions'],
    })).rejects.toThrow(/Can't find join path to join/);

    // The same segment is fine as long as only its own cube is queried.
    const sql = await buildSql({
      measures: ['retail_analysis.transactions_without_returns'],
      dimensions: ['retail_analysis.region'],
      segments: ['retail_analysis.net_sale_transactions'],
    });
    expect(sql).toContain('"sales_line_item".transaction_type NOT IN (\'RETURN\', \'EXCHANGE\')');
  });

  it('is not planned by the legacy planner', async () => {
    await expect(buildSql({
      measures: ['retail_analysis.aov_basket'],
      dimensions: ['retail_analysis.region'],
    }, false)).rejects.toThrow(/Can't find join path to join/);
  });
});
