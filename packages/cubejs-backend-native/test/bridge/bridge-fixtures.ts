// Reference JS-side shapes for every macro-generated bridge.
//
// Each factory returns a fresh value that must satisfy:
//   1. `NativeX::try_new` — required trait fields and required serde-static
//      fields must be present with the right types.
//   2. Per-bridge invoke dispatcher in bridge_test_exports.rs — every
//      `field` getter must deserialize successfully, and every `call` method
//      stub must return a value that marshals back into the declared Rust
//      return type.
//
// Treat these factories as the executable contract documenting what
// schema-compiler and friends are expected to hand to Tesseract for each
// bridge. If a Rust trait gains a new method, the bridge_registry guard
// will fire from the Rust side; if a JS shape stops matching, the invoke
// check fires here.
//
// Keys are JS-side identifiers (post-`#[serde(rename)]`, camelCase for trait
// methods that the macro auto-converts).

/* eslint-disable @typescript-eslint/no-empty-function */

export const memberSqlFn = (): unknown => () => 'sql';

export const filterGroupFixture = (): unknown => ({});
export const filterParamsFixture = (): unknown => ({});
export const securityContextFixture = (): unknown => ({});
export const sqlUtilsFixture = (): unknown => ({});
export const preAggregationObjFixture = (): unknown => ({});

export const geoItemFixture = (): unknown => ({
  sql: memberSqlFn(),
});

export const structWithSqlMemberFixture = (): unknown => ({
  sql: memberSqlFn(),
});

// CaseElseItem.label -> StringOrSql; deserializer tries String first.
export const caseElseItemFixture = (): unknown => ({
  label: 'else',
});

export const caseItemFixture = (): unknown => ({
  sql: memberSqlFn(),
  label: 'when_label',
});

export const caseDefinitionFixture = (): unknown => ({
  when: [caseItemFixture()],
  // CaseDefinition.else_label is renamed to "else" via #[nbridge(rename = "else")].
  else: caseElseItemFixture(),
});

export const caseSwitchElseItemFixture = (): unknown => ({
  sql: memberSqlFn(),
});

export const caseSwitchItemFixture = (): unknown => ({
  value: 'v',
  sql: memberSqlFn(),
});

export const caseSwitchDefinitionFixture = (): unknown => ({
  switch: memberSqlFn(),
  when: [caseSwitchItemFixture()],
  // CaseSwitchDefinition.else_sql is renamed to "else" via #[nbridge(rename = "else")].
  else: caseSwitchElseItemFixture(),
});

export const memberOrderByFixture = (): unknown => ({
  sql: memberSqlFn(),
  dir: 'asc',
});

export const memberDefinitionFixture = (): unknown => ({
  type: 'dimension',
  // sql is optional
});

export const segmentDefinitionFixture = (): unknown => ({
  sql: memberSqlFn(),
  // segment_type, owned_by_cube optional
});

export const joinItemDefinitionFixture = (): unknown => ({
  relationship: 'many_to_one',
  sql: memberSqlFn(),
});

export const joinItemFixture = (): unknown => ({
  from: 'orders',
  to: 'users',
  originalFrom: 'orders',
  originalTo: 'users',
  join: joinItemDefinitionFixture(),
});

export const joinDefinitionFixture = (): unknown => ({
  root: 'orders',
  multiplicationFactor: {},
  joins: [joinItemFixture()],
});

export const joinGraphFixture = (): unknown => ({
  buildJoin: () => joinDefinitionFixture(),
});

export const granularityDefinitionFixture = (): unknown => ({
  interval: '1 day',
  // origin, offset optional
  sql: memberSqlFn(),
});

export const timeShiftDefinitionFixture = (): unknown => ({
  // all static optional, sql optional
  sql: memberSqlFn(),
  interval: '1 day',
  type: 'prior',
  name: 'last_day',
});

export const preAggregationTimeDimensionFixture = (): unknown => ({
  granularity: 'day',
  dimension: memberSqlFn(),
});

export const preAggregationDescriptionFixture = (): unknown => ({
  name: 'main',
  type: 'rollup',
  // granularity, sqlAlias, external, allowNonStrictDateRangeMatch optional
  // measure_references, dimension_references, etc — all optional getters
});

export const viewFilterDefinitionFixture = (): unknown => ({
  operator: 'equals',
  memberReference: 'orders.currency',
  // Values are stringified by CubeEvaluator.prepareViewFilters before reaching
  // Tesseract; nulls are kept to exercise the Option<Vec<Option<String>>> shape.
  valuesReferences: ['USD', null],
  unlessReferences: ['orders.currency'],
});

export const cubeDefinitionFixture = (): unknown => ({
  name: 'Orders',
  // sqlAlias, isView, isCalendar, joinMap optional
  // sql_table, sql optional getters
  filters: [viewFilterDefinitionFixture()],
});

export const dimensionDefinitionFixture = (): unknown => ({
  type: 'string',
  // owned_by_cube, multi_stage, etc. — all optional
  // sql/case/latitude/longitude/time_shift/mask_sql — all optional getters
});

export const measureDefinitionFixture = (): unknown => ({
  type: 'count',
  // owned_by_cube, multi_stage, reduce_by_references, etc. — all optional
  // sql/case/filters/drill_filters/order_by/mask_sql — all optional getters
});

export const expressionStructFixture = (): unknown => ({
  type: 'PatchMeasure',
  // sourceMeasure, replaceAggregationType, addFilters — all optional
});

export const memberExpressionDefinitionFixture = (): unknown => ({
  // expressionName, name, cubeName, definition — all optional
  // expression — required, MemberExpressionExpressionDef tries MemberSql first
  expression: memberSqlFn(),
});

// CubeEvaluator: every method is a `call`. Each stub must return a value
// that marshals into the declared Rust return type. The cascade is real —
// measureByPath has to hand back something that NativeMeasureDefinition
// can wrap, and so on.
export const cubeEvaluatorFixture = (): unknown => ({
  primaryKeys: {},
  parsePath: () => [],
  measureByPath: () => measureDefinitionFixture(),
  dimensionByPath: () => dimensionDefinitionFixture(),
  segmentByPath: () => segmentDefinitionFixture(),
  cubeFromPath: () => cubeDefinitionFixture(),
  isMeasure: () => false,
  isDimension: () => false,
  isSegment: () => false,
  cubeExists: () => false,
  resolveGranularity: () => granularityDefinitionFixture(),
  preAggregationsForCubeAsArray: () => [preAggregationDescriptionFixture()],
  preAggregationDescriptionByName: () => preAggregationDescriptionFixture(),
  // evaluate_rollup_references is invoke-skipped on the Rust side because
  // its `Rc<dyn MemberSql>` argument has no auto-default, but the JS object
  // still needs the key for try_new's has_field check.
  evaluateRollupReferences: () => [],
});

export const driverToolsFixture = (): unknown => ({
  convertTz: () => 'tz',
  timeGroupedColumn: () => 'col',
  sqlTemplates: () => ({}),
  timestampPrecision: () => 6,
  timeStampCast: () => 'ts',
  dateTimeCast: () => 'dt',
  inDbTimeZone: () => 'tz',
  getAllocatedParams: () => [],
  subtractInterval: () => 'd',
  addInterval: () => 'd',
  intervalString: () => 's',
  addTimestampInterval: () => 'd',
  intervalAndMinimalTimeUnit: () => ['1', 'day'],
  hllInit: () => 'h',
  hllMerge: () => 'h',
  hllCardinalityMerge: () => 'h',
  countDistinctApprox: () => 'c',
  supportGeneratedSeriesForCustomTd: () => false,
  dateBin: () => 'b',
});

export const baseToolsFixture = (): unknown => ({
  driverTools: () => driverToolsFixture(),
  sqlTemplates: () => ({}),
  sqlUtilsForRust: () => sqlUtilsFixture(),
  generateTimeSeries: () => [],
  generateCustomTimeSeries: () => [],
  getAllocatedParams: () => [],
  allCubeMembers: () => [],
  intervalAndMinimalTimeUnit: () => ['1', 'day'],
  getPreAggregationByName: () => preAggregationObjFixture(),
  preAggregationTableName: () => 'pre_aggr_table',
  joinTreeForHints: () => joinDefinitionFixture(),
});

export const baseQueryOptionsFixture = (): unknown => ({
  // Static fields
  exportAnnotatedSql: false,
  disableExternalPreAggregations: false,
  // Optional static — omitted intentionally; serde fills None.
  //
  // Trait fields (all `field, optional, vec` except the four required ones)
  cubeEvaluator: cubeEvaluatorFixture(),
  baseTools: baseToolsFixture(),
  joinGraph: joinGraphFixture(),
  securityContext: securityContextFixture(),
  // Optional vec fields can be omitted
});

export type BridgeFixtureFactory = () => unknown;

export const FIXTURES: Record<string, BridgeFixtureFactory> = {
  baseQueryOptions: baseQueryOptionsFixture,
  baseTools: baseToolsFixture,
  caseDefinition: caseDefinitionFixture,
  caseElseItem: caseElseItemFixture,
  caseItem: caseItemFixture,
  caseSwitchDefinition: caseSwitchDefinitionFixture,
  caseSwitchElseItem: caseSwitchElseItemFixture,
  caseSwitchItem: caseSwitchItemFixture,
  cubeDefinition: cubeDefinitionFixture,
  cubeEvaluator: cubeEvaluatorFixture,
  dimensionDefinition: dimensionDefinitionFixture,
  driverTools: driverToolsFixture,
  expressionStruct: expressionStructFixture,
  filterGroup: filterGroupFixture,
  filterParams: filterParamsFixture,
  geoItem: geoItemFixture,
  granularityDefinition: granularityDefinitionFixture,
  joinDefinition: joinDefinitionFixture,
  joinGraph: joinGraphFixture,
  joinItem: joinItemFixture,
  joinItemDefinition: joinItemDefinitionFixture,
  measureDefinition: measureDefinitionFixture,
  memberDefinition: memberDefinitionFixture,
  memberExpressionDefinition: memberExpressionDefinitionFixture,
  memberOrderBy: memberOrderByFixture,
  preAggregationDescription: preAggregationDescriptionFixture,
  preAggregationObj: preAggregationObjFixture,
  preAggregationTimeDimension: preAggregationTimeDimensionFixture,
  securityContext: securityContextFixture,
  segmentDefinition: segmentDefinitionFixture,
  sqlUtils: sqlUtilsFixture,
  structWithSqlMember: structWithSqlMemberFixture,
  timeShiftDefinition: timeShiftDefinitionFixture,
  viewFilterDefinition: viewFilterDefinitionFixture,
};
