import {
  getEnv,
} from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Reproduces a real-world scenario: two fact cubes (sales and share
// metrics), each exposing rolling-window metrics through multi-stage `case`
// entrypoint measures dispatched by a `type: switch` dimension (calc group),
// combined in one view and accelerated with rollup pre-aggregations.
//
// When every cube declares its OWN switch dimension, a query that combines
// measures from both cubes can only pin one of the switches with a filter;
// the other stays unresolved and falls through to its cross-joined
// enumeration. Hosting the switch dimension on a shared single-row cube
// (joined with `1 = 1` into both fact cubes) makes all case entrypoints
// dispatch on the SAME dimension, so a single filter resolves every measure.
//
// Calc-group dimensions are virtual (no stored data), so rollups serve them
// whether or not they are listed in the rollup definition: the pinned value
// renders as a literal over the rollup scan, an unresolved enumeration is
// re-cross-joined — same semantics as over the raw source.

const ROLLING_WINDOW_DIM_CUBE = `
  - name: rolling_window_dim
    sql: SELECT 1 AS one
    public: false
    dimensions:
      - name: one
        sql: one
        type: number
        primary_key: true
        public: false

      - name: rolling_window
        type: switch
        values:
          - R3
          - YTD
`;

const SALES_SQL = `
      SELECT 'A1' AS account, 'P1' AS product, '2017-01-15T00:00:00.000Z'::timestamptz AS sale_date, 10.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, '2017-02-15T00:00:00.000Z'::timestamptz AS sale_date, 20.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, '2017-03-15T00:00:00.000Z'::timestamptz AS sale_date, 30.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, '2017-04-15T00:00:00.000Z'::timestamptz AS sale_date, 40.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, '2017-05-15T00:00:00.000Z'::timestamptz AS sale_date, 50.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, '2017-06-15T00:00:00.000Z'::timestamptz AS sale_date, 60.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P2' AS product, '2017-01-15T00:00:00.000Z'::timestamptz AS sale_date, 5.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P2' AS product, '2017-02-15T00:00:00.000Z'::timestamptz AS sale_date, 5.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P2' AS product, '2017-03-15T00:00:00.000Z'::timestamptz AS sale_date, 5.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P2' AS product, '2017-04-15T00:00:00.000Z'::timestamptz AS sale_date, 5.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P2' AS product, '2017-05-15T00:00:00.000Z'::timestamptz AS sale_date, 5.0 AS amount UNION ALL
      SELECT 'A1' AS account, 'P2' AS product, '2017-06-15T00:00:00.000Z'::timestamptz AS sale_date, 5.0 AS amount UNION ALL
      SELECT 'A2' AS account, 'P1' AS product, '2017-01-15T00:00:00.000Z'::timestamptz AS sale_date, 1.0 AS amount UNION ALL
      SELECT 'A2' AS account, 'P1' AS product, '2017-02-15T00:00:00.000Z'::timestamptz AS sale_date, 1.0 AS amount UNION ALL
      SELECT 'A2' AS account, 'P1' AS product, '2017-03-15T00:00:00.000Z'::timestamptz AS sale_date, 1.0 AS amount UNION ALL
      SELECT 'A2' AS account, 'P1' AS product, '2017-04-15T00:00:00.000Z'::timestamptz AS sale_date, 1.0 AS amount UNION ALL
      SELECT 'A2' AS account, 'P1' AS product, '2017-05-15T00:00:00.000Z'::timestamptz AS sale_date, 1.0 AS amount UNION ALL
      SELECT 'A2' AS account, 'P1' AS product, '2017-06-15T00:00:00.000Z'::timestamptz AS sale_date, 1.0 AS amount
`;

const SHARE_METRICS_SQL = `
      SELECT 'A1' AS account, 'P1' AS product, 'P1' AS competitor_product, '2017-01-15T00:00:00.000Z'::timestamptz AS sale_date, 1.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'P1' AS competitor_product, '2017-02-15T00:00:00.000Z'::timestamptz AS sale_date, 2.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'P1' AS competitor_product, '2017-03-15T00:00:00.000Z'::timestamptz AS sale_date, 3.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'P1' AS competitor_product, '2017-04-15T00:00:00.000Z'::timestamptz AS sale_date, 4.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'P1' AS competitor_product, '2017-05-15T00:00:00.000Z'::timestamptz AS sale_date, 5.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'P1' AS competitor_product, '2017-06-15T00:00:00.000Z'::timestamptz AS sale_date, 6.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'Q' AS competitor_product, '2017-01-15T00:00:00.000Z'::timestamptz AS sale_date, 9.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'Q' AS competitor_product, '2017-02-15T00:00:00.000Z'::timestamptz AS sale_date, 8.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'Q' AS competitor_product, '2017-03-15T00:00:00.000Z'::timestamptz AS sale_date, 7.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'Q' AS competitor_product, '2017-04-15T00:00:00.000Z'::timestamptz AS sale_date, 6.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'Q' AS competitor_product, '2017-05-15T00:00:00.000Z'::timestamptz AS sale_date, 5.0 AS qty UNION ALL
      SELECT 'A1' AS account, 'P1' AS product, 'Q' AS competitor_product, '2017-06-15T00:00:00.000Z'::timestamptz AS sale_date, 4.0 AS qty
`;

// switchRef is the member the case entrypoints dispatch on:
// shared model: '{rolling_window_dim.rolling_window}', per-cube model: '{CUBE.rolling_window}'.
// rollingWindowPreAggDim is the rolling window dimension stored in rollups.
// extra is injected into the cube body (own switch dimension and/or joins).
function salesCube(switchRef: string, rollingWindowPreAggDim: string, extraJoins: string, extraDimensions: string, includePreAggs: boolean = true) {
  return `
  - name: sales
    sql: >${SALES_SQL}
    public: false
    joins:${extraJoins}

    dimensions:
      - name: id
        sql: "{CUBE}.account || '|' || {CUBE}.product || '|' || CAST({CUBE}.sale_date AS TEXT)"
        type: string
        primary_key: true
        public: false

      - name: account
        sql: account
        type: string

      - name: product
        sql: product
        type: string

      - name: date
        sql: sale_date
        type: time
${extraDimensions}
    measures:
      - name: total
        sql: amount
        type: sum

      - name: r3_amount
        sql: amount
        type: sum
        public: false
        rolling_window:
          trailing: 3 month

      - name: prev_r3_amount
        multi_stage: true
        sql: "{r3_amount}"
        type: number
        public: false
        time_shift:
          - interval: 3 month
            type: prior

      - name: r3_amount_change
        multi_stage: true
        type: number
        public: false
        sql: "({r3_amount} - {prev_r3_amount})"

      - name: ytd_amount
        sql: amount
        type: sum
        public: false
        rolling_window:
          type: to_date
          granularity: year

      - name: prev_ytd_amount
        multi_stage: true
        sql: "{ytd_amount}"
        type: number
        public: false
        time_shift:
          - interval: 1 year
            type: prior

      - name: ytd_amount_change
        multi_stage: true
        type: number
        public: false
        sql: "({ytd_amount} - {prev_ytd_amount})"

      - name: rolling_amount
        multi_stage: true
        type: number
        case:
          switch: "${switchRef}"
          when:
            - value: R3
              sql: "{CUBE.r3_amount}"
          else:
            sql: "{CUBE.ytd_amount}"

      - name: rolling_amount_change
        multi_stage: true
        type: number
        case:
          switch: "${switchRef}"
          when:
            - value: R3
              sql: "{CUBE.r3_amount_change}"
          else:
            sql: "{CUBE.ytd_amount_change}"

${includePreAggs ? `
    pre_aggregations:
      - name: perf_rolling
        measures:
          - total
          - r3_amount
          - ytd_amount
        dimensions:
          - account
          - product${rollingWindowPreAggDim ? `
          - ${rollingWindowPreAggDim}` : ''}
        time_dimension: date
        granularity: month
        allow_non_strict_date_range_match: true
` : ''}
`;
}

function shareMetricsCube(switchRef: string, rollingWindowPreAggDim: string, extraJoins: string, extraDimensions: string, includePreAggs: boolean = true, preAggTimeDimension: string = 'date') {
  return `
  - name: share_metrics
    sql: >${SHARE_METRICS_SQL}
    public: false
    joins:
      - name: sales
        sql: "{CUBE}.account = {sales.account} AND {CUBE}.product = {sales.product} AND {CUBE}.sale_date = {sales.date}"
        relationship: many_to_one${extraJoins}

    dimensions:
      - name: id
        sql: "{CUBE}.account || '|' || {CUBE}.product || '|' || {CUBE}.competitor_product || '|' || CAST({CUBE}.sale_date AS TEXT)"
        type: string
        primary_key: true
        public: false

      - name: account
        sql: account
        type: string

      - name: product
        sql: product
        type: string

      - name: competitor_product
        sql: competitor_product
        type: string

      - name: date
        sql: sale_date
        type: time
${extraDimensions}
    measures:
      - name: numerator_r3
        type: sum
        public: false
        rolling_window:
          trailing: 3 month
        sql: "CASE WHEN {CUBE}.competitor_product = {CUBE}.product THEN {CUBE}.qty ELSE 0 END"

      - name: denominator_r3
        type: sum
        public: false
        rolling_window:
          trailing: 3 month
        sql: qty

      - name: numerator_r3_new
        multi_stage: true
        sql: "{numerator_r3}"
        type: number
        public: false

      - name: denominator_r3_new
        multi_stage: true
        sql: "{denominator_r3}"
        type: number
        public: false

      - name: share_r3
        multi_stage: true
        type: number
        public: false
        sql: "CASE WHEN {denominator_r3_new} = 0 THEN NULL ELSE {numerator_r3_new} / {denominator_r3_new} END"

      - name: prev_numerator_r3
        multi_stage: true
        sql: "{numerator_r3}"
        type: number
        public: false
        time_shift:
          - time_dimension: date
            interval: 3 month
            type: prior

      - name: prev_denominator_r3
        multi_stage: true
        sql: "{denominator_r3}"
        type: number
        public: false
        time_shift:
          - time_dimension: date
            interval: 3 month
            type: prior

      - name: prev_share_r3
        multi_stage: true
        type: number
        public: false
        sql: "CASE WHEN {prev_denominator_r3} = 0 THEN NULL ELSE {prev_numerator_r3} / {prev_denominator_r3} END"

      - name: share_change_r3
        multi_stage: true
        type: number
        public: false
        sql: "({share_r3} - {prev_share_r3})"

      - name: numerator_ytd
        type: sum
        public: false
        rolling_window:
          type: to_date
          granularity: year
        sql: "CASE WHEN {CUBE}.competitor_product = {CUBE}.product THEN {CUBE}.qty ELSE 0 END"

      - name: denominator_ytd
        type: sum
        public: false
        rolling_window:
          type: to_date
          granularity: year
        sql: qty

      - name: numerator_ytd_new
        multi_stage: true
        sql: "{numerator_ytd}"
        type: number
        public: false

      - name: denominator_ytd_new
        multi_stage: true
        sql: "{denominator_ytd}"
        type: number
        public: false

      - name: share_ytd
        multi_stage: true
        type: number
        public: false
        sql: "CASE WHEN {denominator_ytd_new} = 0 THEN NULL ELSE {numerator_ytd_new} / {denominator_ytd_new} END"

      - name: prev_numerator_ytd
        multi_stage: true
        sql: "{numerator_ytd}"
        type: number
        public: false
        time_shift:
          - time_dimension: date
            interval: 1 year
            type: prior

      - name: prev_denominator_ytd
        multi_stage: true
        sql: "{denominator_ytd}"
        type: number
        public: false
        time_shift:
          - time_dimension: date
            interval: 1 year
            type: prior

      - name: prev_share_ytd
        multi_stage: true
        type: number
        public: false
        sql: "CASE WHEN {prev_denominator_ytd} = 0 THEN NULL ELSE {prev_numerator_ytd} / {prev_denominator_ytd} END"

      - name: share_change_ytd
        multi_stage: true
        type: number
        public: false
        sql: "({share_ytd} - {prev_share_ytd})"

      - name: rolling_share_change
        multi_stage: true
        type: number
        case:
          switch: "${switchRef}"
          when:
            - value: R3
              sql: "{CUBE.share_change_r3}"
          else:
            sql: "{CUBE.share_change_ytd}"

${includePreAggs ? `
    pre_aggregations:
      - name: perf_share
        measures:
          - numerator_r3
          - denominator_r3
          - numerator_ytd
          - denominator_ytd
        dimensions:
          - account
          - product
          - sales.account
          - sales.product${rollingWindowPreAggDim ? `
          - ${rollingWindowPreAggDim}` : ''}
        time_dimension: ${preAggTimeDimension}
        granularity: month
        allow_non_strict_date_range_match: true
` : ''}
`;
}

const SHARED_JOIN = `
      - name: rolling_window_dim
        sql: "1 = 1"
        relationship: many_to_one
`;

const OWN_SWITCH_DIMENSION = `
      - name: rolling_window
        type: switch
        values:
          - R3
          - YTD
`;

// Model where the rolling window selector lives on one shared calc-group
// cube joined into both fact cubes: one filter drives every case measure.
const sharedSwitchModel = `
cubes:
${ROLLING_WINDOW_DIM_CUBE}
${salesCube('{rolling_window_dim.rolling_window}', 'rolling_window_dim.rolling_window', SHARED_JOIN, '')}
${shareMetricsCube('{rolling_window_dim.rolling_window}', 'rolling_window_dim.rolling_window', SHARED_JOIN, '')}
views:
  - name: performance_view
    cubes:
      - join_path: sales
        includes:
          - account
          - product
          - date
          - total
          - rolling_amount
          - rolling_amount_change

      - join_path: share_metrics
        includes:
          - rolling_share_change

      - join_path: rolling_window_dim
        includes:
          - rolling_window
`;

// Shared calc-group switch, and the share_metrics rollup is keyed on the
// time dimension the query actually asks for (`sales.date`, exposed as
// `date` in the view) instead of the cube's own `share_metrics.date`.
const sharedSwitchModelSharedDateRollups = `
cubes:
${ROLLING_WINDOW_DIM_CUBE}
${salesCube('{rolling_window_dim.rolling_window}', 'rolling_window_dim.rolling_window', SHARED_JOIN, '')}
${shareMetricsCube('{rolling_window_dim.rolling_window}', 'rolling_window_dim.rolling_window', SHARED_JOIN, '', true, 'sales.date')}
views:
  - name: performance_view
    cubes:
      - join_path: sales
        includes:
          - account
          - product
          - date
          - total
          - rolling_amount
          - rolling_amount_change

      - join_path: share_metrics
        includes:
          - rolling_share_change
          - name: date
            alias: ms_date

      - join_path: rolling_window_dim
        includes:
          - rolling_window
`;

// Model where each fact cube declares its OWN switch dimension: the view
// filter pins only the sales switch, the share_metrics one stays
// unresolved and pre-aggregations can't match.
const perCubeSwitchModel = `
cubes:
${salesCube('{CUBE.rolling_window}', 'rolling_window', '', OWN_SWITCH_DIMENSION)}
${shareMetricsCube('{CUBE.rolling_window}', 'rolling_window', '', OWN_SWITCH_DIMENSION)}
views:
  - name: performance_view
    cubes:
      - join_path: sales
        includes:
          - account
          - product
          - date
          - total
          - rolling_amount
          - rolling_amount_change
          - rolling_window

      - join_path: share_metrics
        includes:
          - rolling_share_change
          - name: rolling_window
            alias: share_metrics_rolling_window
`;

// Shared calc-group switch, but the rollups do NOT store the calc-group
// dimension: the planner must still match them (the dimension is virtual)
// and resolve the filtered value as a literal over the rollup scan.
const sharedSwitchModelSlimRollups = `
cubes:
${ROLLING_WINDOW_DIM_CUBE}
${salesCube('{rolling_window_dim.rolling_window}', '', SHARED_JOIN, '')}
${shareMetricsCube('{rolling_window_dim.rolling_window}', '', SHARED_JOIN, '')}
views:
  - name: performance_view
    cubes:
      - join_path: sales
        includes:
          - account
          - product
          - date
          - total
          - rolling_amount
          - rolling_amount_change

      - join_path: share_metrics
        includes:
          - rolling_share_change

      - join_path: rolling_window_dim
        includes:
          - rolling_window
`;

// Per-cube switch model without pre-aggregations: used to demonstrate the
// semantic problem of the anti-pattern with deterministic values — the view
// filter pins only the sales switch, so share_metrics computes its case
// measure across its whole cross-joined enumeration.
const perCubeSwitchModelNoPreAggs = `
cubes:
${salesCube('{CUBE.rolling_window}', 'rolling_window', '', OWN_SWITCH_DIMENSION, false)}
${shareMetricsCube('{CUBE.rolling_window}', 'rolling_window', '', OWN_SWITCH_DIMENSION, false)}
views:
  - name: performance_view
    cubes:
      - join_path: sales
        includes:
          - account
          - product
          - date
          - total
          - rolling_amount
          - rolling_amount_change
          - rolling_window

      - join_path: share_metrics
        includes:
          - rolling_share_change
          - name: rolling_window
            alias: share_metrics_rolling_window
          - name: date
            alias: ms_date
`;

// Same model without pre-aggregations: used to verify the plain-SQL results
// of the repro query deterministically (rolling windows anchored to the
// query date range instead of the current date).
const sharedSwitchModelNoPreAggs = `
cubes:
${ROLLING_WINDOW_DIM_CUBE}
${salesCube('{rolling_window_dim.rolling_window}', 'rolling_window_dim.rolling_window', SHARED_JOIN, '', false)}
${shareMetricsCube('{rolling_window_dim.rolling_window}', 'rolling_window_dim.rolling_window', SHARED_JOIN, '', false)}
views:
  - name: performance_view
    cubes:
      - join_path: sales
        includes:
          - account
          - product
          - date
          - total
          - rolling_amount
          - rolling_amount_change

      - join_path: share_metrics
        includes:
          - rolling_share_change
          - name: date
            alias: ms_date

      - join_path: rolling_window_dim
        includes:
          - rolling_window
`;

const REPRO_QUERY = {
  measures: [
    'performance_view.rolling_amount',
    'performance_view.rolling_amount_change',
    'performance_view.rolling_share_change',
  ],
  dimensions: ['performance_view.product'],
  filters: [
    { member: 'performance_view.account', operator: 'equals', values: ['A1'] },
    { member: 'performance_view.rolling_window', operator: 'equals', values: ['R3'] },
  ],
  timezone: 'UTC',
  order: [{ id: 'performance_view.product' }],
  preAggregationsSchema: '',
  cubestoreSupportMultistage: true,
};

const SALES_ROLLING_MEASURES = [
  'performance_view.rolling_amount',
  'performance_view.rolling_amount_change',
];

// A month-granularity time dimension over a bounded date range: the shape a
// dashboard charting these rolling metrics per month actually sends. Both
// rollups are built at `granularity: month` on the same date, so the query
// must be served from them.
const MONTH_TIME_DIMENSION = [{
  dimension: 'performance_view.date',
  granularity: 'month',
  dateRange: ['2017-04-01', '2017-06-30'],
}];

const MONTH_QUERY = {
  ...REPRO_QUERY,
  timeDimensions: MONTH_TIME_DIMENSION,
  order: [{ id: 'performance_view.product' }, { id: 'performance_view.date' }],
};

// Monthly R3 (trailing 3 month) values and their 3-month-prior change for
// account A1: P1 sells 10/20/30/40/50/60 from January to June, P2 sells 5
// every month.
//   R3(Apr) = Feb+Mar+Apr, R3(May) = Mar+Apr+May, R3(Jun) = Apr+May+Jun
//   change(m) = R3(m) - R3(m - 3 month)
const EXPECTED_MONTHLY_SALES_ROWS = [
  {
    performance_view__product: 'P1',
    performance_view__date_month: '2017-04-01T00:00:00.000Z',
    performance_view__rolling_amount: '90.0',
    performance_view__rolling_amount_change: '80.0',
  },
  {
    performance_view__product: 'P1',
    performance_view__date_month: '2017-05-01T00:00:00.000Z',
    performance_view__rolling_amount: '120.0',
    performance_view__rolling_amount_change: '90.0',
  },
  {
    performance_view__product: 'P1',
    performance_view__date_month: '2017-06-01T00:00:00.000Z',
    performance_view__rolling_amount: '150.0',
    performance_view__rolling_amount_change: '90.0',
  },
  {
    performance_view__product: 'P2',
    performance_view__date_month: '2017-04-01T00:00:00.000Z',
    performance_view__rolling_amount: '15.0',
    performance_view__rolling_amount_change: '10.0',
  },
  {
    performance_view__product: 'P2',
    performance_view__date_month: '2017-05-01T00:00:00.000Z',
    performance_view__rolling_amount: '15.0',
    performance_view__rolling_amount_change: '5.0',
  },
  {
    performance_view__product: 'P2',
    performance_view__date_month: '2017-06-01T00:00:00.000Z',
    performance_view__rolling_amount: '15.0',
    performance_view__rolling_amount_change: '0.0',
  },
];

describe('PreAggregationsSharedCalcGroup', () => {
  jest.setTimeout(200000);

  if (getEnv('nativeSqlPlanner')) {
    describe('shared calc-group switch dimension', () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(sharedSwitchModel);

      it('matches rollups without a time dimension in the query', () => compiler.compile().then(() => {
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, REPRO_QUERY);

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const sqlAndParams = query.buildSqlAndParams();
        const tableNames = preAggregationsDescription.map((d: any) => d.tableName);
        expect(tableNames).toContain('sales_perf_rolling');
        expect(tableNames).toContain('share_metrics_perf_share');
        expect(sqlAndParams[0]).toContain('sales_perf_rolling');
        expect(sqlAndParams[0]).toContain('share_metrics_perf_share');

        // Rolling windows without a date range are anchored to the current
        // date, so only assert the query is served by the rollups end to end.
        return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
          expect(Array.isArray(res)).toBe(true);
        });
      }));

      // Adding a month-granularity time dimension to the same query keeps the
      // sales rollup: the rollup is built at `granularity: month` on the very
      // dimension the query groups by.
      it('matches its own rollup with a month time dimension for single-cube measures', () => compiler.compile().then(() => {
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
          ...MONTH_QUERY,
          measures: SALES_ROLLING_MEASURES,
        });

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const sqlAndParams = query.buildSqlAndParams();
        const tableNames = preAggregationsDescription.map((d: any) => d.tableName);
        expect(tableNames).toEqual(['sales_perf_rolling']);
        expect(sqlAndParams[0]).toContain('sales_perf_rolling');

        return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
          expect(res).toEqual(EXPECTED_MONTHLY_SALES_ROWS);
        });
      }));

      // Cross-cube with a time dimension is where matching breaks in this
      // model: `share_metrics.perf_share` is keyed on `share_metrics.date`,
      // while the view exposes (and the query asks for) `sales.date`. The two
      // are equal by the join condition, but the matcher only knows stored
      // members, so the share rollup can't serve the query — and because
      // matching is all-or-nothing across the multi-cube query, the sales
      // rollup is dropped as well and everything is recomputed from the raw
      // source. Results stay correct; only the acceleration is lost.
      // FIXME: unskip the sibling spec below (rollups keyed on the queried
      // time dimension) as the reference behaviour and teach the matcher to
      // resolve join-equal time dimensions.
      it('loses both rollups for cross-cube measures when the joined rollup is keyed on its own time dimension', () => compiler.compile().then(() => {
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, MONTH_QUERY);

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const sqlAndParams = query.buildSqlAndParams();
        expect(preAggregationsDescription.map((d: any) => d.tableName)).toEqual([]);
        expect(sqlAndParams[0]).not.toContain('sales_perf_rolling');
        expect(sqlAndParams[0]).not.toContain('share_metrics_perf_share');
      }));
    });

    // Same shared calc-group model, but `share_metrics.perf_share` is keyed on
    // the time dimension the query actually groups by (`sales.date`, exposed
    // as `date` in the view) instead of the cube's own `share_metrics.date`.
    describe('shared calc-group switch dimension with rollups keyed on the queried time dimension', () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(sharedSwitchModelSharedDateRollups);
      const noPreAggs = prepareYamlCompiler(sharedSwitchModelNoPreAggs);

      it('matches both rollups for cross-cube rolling measures with a month time dimension', () => compiler.compile().then(() => {
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, MONTH_QUERY);

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const sqlAndParams = query.buildSqlAndParams();
        const tableNames = preAggregationsDescription.map((d: any) => d.tableName);
        expect(tableNames).toContain('sales_perf_rolling');
        expect(tableNames).toContain('share_metrics_perf_share');
        expect(sqlAndParams[0]).toContain('sales_perf_rolling');
        expect(sqlAndParams[0]).toContain('share_metrics_perf_share');

        return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
          // The sales side is pinned to exact values; the share-of-total side
          // is compared against the same query over the raw source below.
          expect(res).toEqual(EXPECTED_MONTHLY_SALES_ROWS.map((row, i) => ({
            ...row,
            performance_view__rolling_share_change: i < 3 ? '0.00000000000000000000' : null,
          })));
        });
      }));

      // The rollups must not change the answer: the same query over the same
      // model without pre-aggregations returns exactly the same rows.
      it('returns the same rows from the rollups as from the raw source', async () => {
        await compiler.compile();
        await noPreAggs.compiler.compile();

        const fromRollups = await dbRunner.evaluateQueryWithPreAggregations(
          new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, MONTH_QUERY)
        );
        const fromSource = await dbRunner.testQuery(
          new PostgresQuery({
            joinGraph: noPreAggs.joinGraph,
            cubeEvaluator: noPreAggs.cubeEvaluator,
            compiler: noPreAggs.compiler,
          }, MONTH_QUERY).buildSqlAndParams()
        );

        expect(fromRollups).toEqual(fromSource);
      });

      it('matches both rollups with a date-range-only time dimension', () => compiler.compile().then(() => {
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
          ...REPRO_QUERY,
          timeDimensions: [{
            dimension: 'performance_view.date',
            dateRange: ['2017-04-01', '2017-06-30'],
          }],
        });

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const sqlAndParams = query.buildSqlAndParams();
        const tableNames = preAggregationsDescription.map((d: any) => d.tableName);
        expect(tableNames).toContain('sales_perf_rolling');
        expect(tableNames).toContain('share_metrics_perf_share');
        expect(sqlAndParams[0]).toContain('sales_perf_rolling');
        expect(sqlAndParams[0]).toContain('share_metrics_perf_share');
      }));

      // FIXME: expressing the same range as an `inDateRange` filter on the
      // time dimension instead of `timeDimensions[].dateRange` stops every
      // rollup from matching (even when `dateRange` is also present): the
      // filter carries a plain dimension symbol, and the matcher looks it up
      // only among the rollup's stored dimensions, never among its time
      // dimensions. Unskip once time-dimension filters are matched against
      // the rollup granularity.
      it.skip('matches rollups when the range is given as an inDateRange filter', () => compiler.compile().then(() => {
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
          ...MONTH_QUERY,
          filters: [
            ...REPRO_QUERY.filters,
            { member: 'performance_view.date', operator: 'inDateRange', values: ['2017-04-01', '2017-06-30'] },
          ],
        });

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const tableNames = preAggregationsDescription.map((d: any) => d.tableName);
        expect(tableNames).toContain('sales_perf_rolling');
        expect(tableNames).toContain('share_metrics_perf_share');
      }));
    });

    describe('shared calc-group switch dimension with slim rollups', () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(sharedSwitchModelSlimRollups);

      // The calc-group dimension is virtual (a cross-joined enumeration),
      // so rollups that don't store it must still match: the filtered value
      // is rendered as a literal over the rollup scan.
      it('matches rollups that do not store the calc-group dimension', () => compiler.compile().then(() => {
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, REPRO_QUERY);

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const sqlAndParams = query.buildSqlAndParams();
        const tableNames = preAggregationsDescription.map((d: any) => d.tableName);
        expect(tableNames).toContain('sales_perf_rolling');
        expect(tableNames).toContain('share_metrics_perf_share');
        expect(sqlAndParams[0]).toContain('sales_perf_rolling');
        expect(sqlAndParams[0]).toContain('share_metrics_perf_share');
        // The rollup build must not cross-join the calc-group values table.
        const loadSql = preAggregationsDescription
          .map((d: any) => d.loadSql[0])
          .join('\n');
        expect(loadSql).not.toContain('rolling_window_values');

        return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
          expect(Array.isArray(res)).toBe(true);
        });
      }));
    });

    describe('shared calc-group switch dimension without rollups', () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(sharedSwitchModelNoPreAggs);

      it('computes deterministic sales rolling values anchored to the date range', async () => {
        await dbRunner.runQueryTest({
          ...REPRO_QUERY,
          measures: [
            'performance_view.rolling_amount',
            'performance_view.rolling_amount_change',
          ],
          timeDimensions: [{
            dimension: 'performance_view.date',
            dateRange: ['2017-01-01', '2017-06-30'],
          }],
        }, [
          {
            performance_view__product: 'P1',
            performance_view__rolling_amount: '150.0',
            performance_view__rolling_amount_change: '90.0',
          },
          {
            performance_view__product: 'P2',
            performance_view__rolling_amount: '15.0',
            performance_view__rolling_amount_change: '0.0',
          },
        ],
        { joinGraph, cubeEvaluator, compiler });
      });

      // Same measures with a month-granularity time dimension: one row per
      // month, each carrying its own trailing-3-month window and the change
      // against the window 3 months earlier. Pins the arithmetic the rollup
      // specs above compare against.
      it('computes deterministic monthly rolling values', async () => {
        await dbRunner.runQueryTest({
          ...MONTH_QUERY,
          measures: SALES_ROLLING_MEASURES,
        }, EXPECTED_MONTHLY_SALES_ROWS,
        { joinGraph, cubeEvaluator, compiler });
      });

      // The time_shift of the share-of-total measures is declared on
      // share_metrics.date, so the anchor date range must be set on that
      // dimension (exposed as ms_date) for the prior window to move.
      it('computes deterministic share-of-total change anchored to its own date range', async () => {
        await dbRunner.runQueryTest({
          ...REPRO_QUERY,
          measures: [
            'performance_view.rolling_share_change',
          ],
          timeDimensions: [{
            dimension: 'performance_view.ms_date',
            dateRange: ['2017-01-01', '2017-06-30'],
          }],
        }, [
          {
            performance_view__product: 'P1',
            performance_view__rolling_share_change: '0.30000000000000000000',
          },
        ],
        { joinGraph, cubeEvaluator, compiler });
      });
    });

    describe('per-cube switch dimensions (anti-pattern)', () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(perCubeSwitchModel);
      const noPreAggs = prepareYamlCompiler(perCubeSwitchModelNoPreAggs);

      // This is what makes per-cube switches an anti-pattern: the view
      // filter pins only the sales switch, while share_metrics falls
      // through to its own cross-joined enumeration, so every enumeration
      // value emits its own result row even though the switch dimension is
      // not projected. The same query against the shared-switch model
      // returns a single row of 0.3 (see the deterministic test above);
      // here a duplicate P1 row from the YTD branch (null: its 1-year prior
      // window has no data) leaks into the result.
      it('produces a divergent share change because one switch stays unresolved', async () => {
        await dbRunner.runQueryTest({
          ...REPRO_QUERY,
          measures: [
            'performance_view.rolling_share_change',
          ],
          timeDimensions: [{
            dimension: 'performance_view.ms_date',
            dateRange: ['2017-01-01', '2017-06-30'],
          }],
        }, [
          {
            performance_view__product: 'P1',
            performance_view__rolling_share_change: '0.30000000000000000000',
          },
          {
            performance_view__product: 'P1',
            performance_view__rolling_share_change: null,
          },
        ],
        { joinGraph: noPreAggs.joinGraph, cubeEvaluator: noPreAggs.cubeEvaluator, compiler: noPreAggs.compiler });
      });

      // Calc-group dimensions are virtual, so rollups serve this query too.
      // The anti-pattern remains semantic: the view filter pins only the
      // sales switch, while the share_metrics switch stays unresolved and
      // falls through to its cross-joined enumeration — same behavior as
      // over the raw source, just accelerated.
      it('cross-cube rolling measures still match rollups with per-cube switches', () => compiler.compile().then(() => {
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, REPRO_QUERY);

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const sqlAndParams = query.buildSqlAndParams();
        const tableNames = preAggregationsDescription.map((d: any) => d.tableName);
        expect(tableNames).toContain('sales_perf_rolling');
        expect(tableNames).toContain('share_metrics_perf_share');
        expect(sqlAndParams[0]).toContain('sales_perf_rolling');
        expect(sqlAndParams[0]).toContain('share_metrics_perf_share');
      }));

      it('single-cube rolling measures still match their own rollup', () => compiler.compile().then(() => {
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
          ...REPRO_QUERY,
          measures: [
            'performance_view.rolling_amount',
            'performance_view.rolling_amount_change',
          ],
        });

        const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
        const sqlAndParams = query.buildSqlAndParams();
        const tableNames = preAggregationsDescription.map((d: any) => d.tableName);
        expect(tableNames).toContain('sales_perf_rolling');
        expect(tableNames).not.toContain('share_metrics_perf_share');
        expect(sqlAndParams[0]).toContain('sales_perf_rolling');
        expect(sqlAndParams[0]).not.toContain('share_metrics_perf_share');
      }));
    });
  } else {
    it.skip('shared calc-group pre-aggregations', () => {
      // Works only with the Tesseract SQL planner
    });
  }
});
