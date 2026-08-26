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
      # References inside a view measure are resolved against the view, so both
      # \`{CUBE.member}\` and \`{view_name.member}\` work - one of each below - while
      # a bare \`{member}\` does not resolve at all. \`multi_stage\` is what lets the
      # ratio be evaluated after both facts have been aggregated.
      - name: aov_basket
        type: number
        multi_stage: true
        sql: "{CUBE.sales_amount} / NULLIF({CUBE.transactions_without_returns}, 0)"
      # Same expression without \`multi_stage\`, kept to pin what happens when
      # the ratio is planned as an ordinary calculated measure.
      - name: aov_basket_single_stage
        type: number
        sql: "{retail_analysis.sales_amount} / NULLIF({retail_analysis.transactions_without_returns}, 0)"
`;

let compilers: any;

beforeAll(async () => {
  compilers = prepareYamlCompiler(model);
  await compilers.compiler.compile();
});

const buildSql = (query: any, useNativeSqlPlanner: boolean = true) => {
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
// The ratio, taken over the two per-fact aggregate columns once they are lined
// up on the query's dimensions. The subquery aliases the planner puts in front
// of those columns are deliberately not pinned - only that the numerator and
// denominator are the aggregated columns, in that order.
const RATIO_OVER_AGGREGATES =
  /"item_location_sales__sales_amount" \/ NULLIF\("[^"]+"\."sales_line_item__transactions_without_returns", 0\)/;

// Multi-fact queries are planned by Tesseract only, so everything that is
// expected to produce SQL runs against the native planner.
describe('Multi-fact derived measure defined on a view', () => {
  it('aggregates each fact cube separately when the components are queried side by side', async () => {
    const sql = buildSql({
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
    const sql = buildSql({
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
    const sql = buildSql({
      measures: ['retail_analysis.aov_basket'],
      timeDimensions: [{ dimension: 'retail_analysis.date', granularity: 'day' }],
    });

    expect(sql).toContain('DATE_TRUNC(\'day\', "item_location_sales".date) = "dates".date');
    expect(sql).toContain('DATE_TRUNC(\'day\', "sales_line_item".sold_at) = "dates".date');
    expect(sql).toMatch(RATIO_OVER_AGGREGATES);
  });

  it('returns the ratio next to its components', async () => {
    const sql = buildSql({
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
    const sql = buildSql({
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
  it('cannot plan the ratio when the view measure is not multi_stage', () => {
    expect(() => buildSql({
      measures: ['retail_analysis.aov_basket_single_stage'],
      dimensions: ['retail_analysis.region'],
    })).toThrow(/Can't find join path to join .*item_location_sales.*sales_line_item/);
  });

  // Current behaviour, pinned. A segment is the other place cube-owned filter
  // logic could be written once and reused; it does not survive the multi-fact
  // split, so shared filter logic has to live in the measure's own `filters:`
  // (which does travel - see the first test).
  it('cannot plan a multi-fact query that carries a segment', () => {
    expect(() => buildSql({
      measures: ['retail_analysis.aov_basket'],
      dimensions: ['retail_analysis.region'],
      segments: ['retail_analysis.net_sale_transactions'],
    })).toThrow(/Can't find join path to join/);
  });

  it('applies the segment when only its own fact is queried', () => {
    const sql = buildSql({
      measures: ['retail_analysis.transactions_without_returns'],
      dimensions: ['retail_analysis.region'],
      segments: ['retail_analysis.net_sale_transactions'],
    });

    expect(sql).toContain('"sales_line_item".transaction_type NOT IN (\'RETURN\', \'EXCHANGE\')');
  });

  it('is not planned by the legacy planner', () => {
    expect(() => buildSql({
      measures: ['retail_analysis.aov_basket'],
      dimensions: ['retail_analysis.region'],
    }, false)).toThrow(/Can't find join path to join/);
  });

  // The reference forms a view measure accepts. `{CUBE.member}` and
  // `{view_name.member}` are both in the model above; a bare `{member}` is
  // rejected while the view is compiled, so it needs a model of its own.
  it('rejects a bare member reference in a view measure', async () => {
    const bareRef = model.replace(
      '{CUBE.sales_amount} / NULLIF({CUBE.transactions_without_returns}, 0)',
      '{sales_amount} / NULLIF({transactions_without_returns}, 0)'
    );

    await expect(prepareYamlCompiler(bareRef).compiler.compile())
      .rejects.toThrow(/sales_amount is not defined/);
  });
});

// The other reason a view measure spanning cubes wants `multi_stage`: even when
// the cubes DO join, a plain calculated measure is evaluated inside the single
// joined scan, so a `sum` on the one side is taken over rows the join has
// multiplied. `multi_stage` aggregates each side first, then divides.
describe('Derived view measure over a fanned-out join', () => {
  const fanOutModel = `
cubes:
  - name: orders
    sql: >
      SELECT 1 AS id, 100 AS amount, 'NYC' AS city UNION ALL
      SELECT 2 AS id, 200 AS amount, 'NYC' AS city
    joins:
      - name: line_items
        sql: "{CUBE}.id = {line_items}.order_id"
        relationship: one_to_many
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
      - name: city
        sql: "{CUBE}.city"
        type: string
    measures:
      - name: total_amount
        sql: "{CUBE}.amount"
        type: sum

  - name: line_items
    sql: >
      SELECT 10 AS id, 1 AS order_id UNION ALL
      SELECT 11 AS id, 1 AS order_id UNION ALL
      SELECT 12 AS id, 2 AS order_id
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
    measures:
      - name: count
        type: count

views:
  - name: orders_overview
    cubes:
      - join_path: orders
        includes:
          - total_amount
          - city
      - join_path: orders.line_items
        includes:
          - count

    measures:
      - name: average_line_value
        type: number
        sql: "{CUBE.total_amount} / NULLIF({CUBE.count}, 0)"
      - name: average_line_value_multi_stage
        type: number
        multi_stage: true
        sql: "{CUBE.total_amount} / NULLIF({CUBE.count}, 0)"
`;

  let fanOutCompilers: any;

  beforeAll(async () => {
    fanOutCompilers = prepareYamlCompiler(fanOutModel);
    await fanOutCompilers.compiler.compile();
  });

  const buildFanOutSql = (measure: string) => {
    const [sql] = new PostgresQuery(fanOutCompilers, {
      timezone: 'UTC',
      useNativeSqlPlanner: true,
      measures: [measure],
      dimensions: ['orders_overview.city'],
    }).buildSqlAndParams();

    return sql;
  };

  // Current behaviour, pinned: `sum` runs over the multiplied rows of the join,
  // so the numerator is larger than the same measure queried on its own.
  it('inlines a plain calculated measure into the multiplied join', () => {
    const sql = buildFanOutSql('orders_overview.average_line_value');

    expect(sql).toMatch(/sum\("orders"\.amount\) \/ NULLIF\(count\("line_items"\.id\), 0\)/);
    expect(sql).toContain('"orders".id = "line_items".order_id');
  });

  it('aggregates each side before dividing when the measure is multi_stage', () => {
    const sql = buildFanOutSql('orders_overview.average_line_value_multi_stage');

    // `sum` is taken in a leg that never joins line_items, so nothing multiplies
    // it. The span is tempered against `line_items` so the assertion fails if
    // that leg ever picks the join back up.
    expect(sql).toMatch(/sum\("orders"\.amount\) "orders__total_amount"(?:(?!line_items)[\s\S])*?GROUP BY 1/);
    expect(sql).not.toMatch(/sum\("orders"\.amount\) \/ NULLIF/);
    // The division happens over the two aggregated columns.
    expect(sql).toMatch(/"orders__total_amount" \/ NULLIF\("[^"]+"\."line_items__count", 0\)/);
  });
});
