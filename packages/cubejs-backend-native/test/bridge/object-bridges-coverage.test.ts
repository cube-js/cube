import {
  bridgeHarnessAvailable,
  expectAllInvocationsOk,
  fieldNames,
  invokeBridge,
  listBridgeFields,
  listBridgeNames,
  parseBridge,
} from './helpers';
import { FIXTURES } from './bridge-fixtures';

// Table-driven coverage for every bridge that uses #[nativebridge::native_bridge].
//
// Each row pins the field set the macro should emit for the bridge,
// keyed by Rust ident (snake_case) — this is the same shape `fieldNames`
// returns. Adding a method or static field on either side without
// updating this list fails the meta assertion.
//
// The fully-populated fixture for the bridge lives in `bridge-fixtures.ts`.
// That file is the executable JS-side contract: the explicit shape we
// expect schema-compiler / cubejs-server-core to hand to Tesseract for
// each bridge. The invoke check below fires every `field` getter and
// every `call` method on the bridge against that fixture, so a mismatch
// between the Rust trait and the JS contract surfaces immediately.
//
// Coverage scope: every trait annotated with #[nativebridge::native_bridge]
// is registered in `bridge_registry!` and pinned here. Hand-rolled
// bridges (MemberSql, FilterParamsCallback, SqlTemplatesRender) live
// outside the macro and are not in scope here.
type BridgeSpec = {
  name: string;
  expected: string[];
};

const BRIDGES: BridgeSpec[] = [
  {
    name: 'baseQueryOptions',
    expected: [
      'base_tools',
      'convert_tz_for_raw_time_dimension',
      'cube_evaluator',
      'cubestore_support_multistage',
      'dimensions',
      'disable_external_pre_aggregations',
      'export_annotated_sql',
      'filters',
      'join_graph',
      'join_hints',
      'limit',
      'masked_members',
      'measures',
      'member_to_alias',
      'offset',
      'order',
      'pre_aggregation_id',
      'pre_aggregation_query',
      'row_limit',
      'security_context',
      'segments',
      'time_dimensions',
      'timezone',
      'total_query',
      'ungrouped',
    ],
  },
  {
    name: 'baseTools',
    expected: [
      'all_cube_members',
      'driver_tools',
      'generate_custom_time_series',
      'generate_time_series',
      'get_allocated_params',
      'get_pre_aggregation_by_name',
      'interval_and_minimal_time_unit',
      'join_tree_for_hints',
      'pre_aggregation_table_name',
      'sql_templates',
      'sql_utils_for_rust',
    ],
  },
  { name: 'caseDefinition', expected: ['else_label', 'when'] },
  { name: 'caseElseItem', expected: ['label'] },
  { name: 'caseItem', expected: ['label', 'sql'] },
  { name: 'caseSwitchDefinition', expected: ['else_sql', 'switch', 'when'] },
  { name: 'caseSwitchElseItem', expected: ['sql'] },
  { name: 'caseSwitchItem', expected: ['sql', 'value'] },
  {
    name: 'cubeDefinition',
    expected: [
      'default_filters',
      'is_calendar',
      'is_view',
      'join_map',
      'name',
      'sql',
      'sql_alias',
      'sql_table',
    ],
  },
  {
    name: 'cubeEvaluator',
    expected: [
      'cube_exists',
      'cube_from_path',
      'dimension_by_path',
      'evaluate_rollup_references',
      'is_dimension',
      'is_measure',
      'is_segment',
      'measure_by_path',
      'parse_path',
      'pre_aggregation_description_by_name',
      'pre_aggregations_for_cube_as_array',
      'primary_keys',
      'resolve_granularity',
      'segment_by_path',
    ],
  },
  {
    name: 'dimensionDefinition',
    expected: [
      'add_group_by_references',
      'case',
      'dimension_type',
      'filter',
      'latitude',
      'longitude',
      'mask_sql',
      'multi_stage',
      'owned_by_cube',
      'primary_key',
      'propagate_filters_to_sub_query',
      'sql',
      'sub_query',
      'time_shift',
      'values',
    ],
  },
  {
    name: 'driverTools',
    expected: [
      'add_interval',
      'add_timestamp_interval',
      'convert_tz',
      'count_distinct_approx',
      'date_bin',
      'date_time_cast',
      'get_allocated_params',
      'hll_cardinality_merge',
      'hll_init',
      'hll_merge',
      'in_db_time_zone',
      'interval_and_minimal_time_unit',
      'interval_string',
      'sql_templates',
      'subtract_interval',
      'support_generated_series_for_custom_td',
      'time_grouped_column',
      'time_stamp_cast',
      'timestamp_precision',
    ],
  },
  {
    name: 'expressionStruct',
    expected: ['add_filters', 'expression_type', 'replace_aggregation_type', 'source_measure'],
  },
  { name: 'filterGroup', expected: [] },
  { name: 'filterParams', expected: [] },
  { name: 'geoItem', expected: ['sql'] },
  { name: 'granularityDefinition', expected: ['interval', 'offset', 'origin', 'sql'] },
  { name: 'joinDefinition', expected: ['joins', 'multiplication_factor', 'root'] },
  { name: 'joinGraph', expected: ['build_join'] },
  { name: 'joinItem', expected: ['from', 'join', 'original_from', 'original_to', 'to'] },
  { name: 'joinItemDefinition', expected: ['relationship', 'sql'] },
  {
    name: 'measureDefinition',
    expected: [
      'add_group_by_references',
      'case',
      'drill_filters',
      'filter',
      'filters',
      'grain',
      'group_by_references',
      'mask_sql',
      'measure_type',
      'multi_stage',
      'order_by',
      'owned_by_cube',
      'reduce_by_references',
      'rolling_window',
      'sql',
      'time_shift_references',
    ],
  },
  { name: 'memberDefinition', expected: ['member_type', 'sql'] },
  {
    name: 'memberExpressionDefinition',
    expected: ['cube_name', 'definition', 'expression', 'expression_name', 'name'],
  },
  { name: 'memberOrderBy', expected: ['dir', 'sql'] },
  {
    name: 'multiStageFilter',
    expected: ['exclude', 'include', 'keep_only', 'mode'],
  },
  {
    name: 'multiStageGrain',
    expected: ['exclude', 'include', 'keep_only', 'mode'],
  },
  {
    name: 'preAggregationDescription',
    expected: [
      'allow_non_strict_date_range_match',
      'dimension_references',
      'external',
      'granularity',
      'measure_references',
      'name',
      'pre_aggregation_type',
      'rollup_references',
      'segment_references',
      'sql_alias',
      'time_dimension_reference',
      'time_dimension_references',
    ],
  },
  {
    name: 'preAggregationObj',
    expected: ['cube', 'pre_aggregation_id', 'pre_aggregation_name', 'table_name'],
  },
  { name: 'preAggregationTimeDimension', expected: ['dimension', 'granularity'] },
  { name: 'securityContext', expected: [] },
  { name: 'segmentDefinition', expected: ['owned_by_cube', 'segment_type', 'sql'] },
  { name: 'sqlUtils', expected: [] },
  { name: 'structWithSqlMember', expected: ['sql'] },
  { name: 'timeShiftDefinition', expected: ['interval', 'name', 'sql', 'timeshift_type'] },
  {
    name: 'viewFilterDefinition',
    expected: ['member_reference', 'operator', 'unless_references', 'values_references'],
  },
];

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

// Cross-side completeness guard. The bridge_registry! macro on the Rust
// side is the source of truth; both `BRIDGES` and `FIXTURES` must enumerate
// the exact same set of bridges. Adding a bridge in Rust without wiring
// the JS contract here (or vice versa) would otherwise be a silent gap.
describeBridge('bridge object: registry coverage', () => {
  it('every registered bridge has a row in BRIDGES and an entry in FIXTURES', () => {
    const registered = [...listBridgeNames()].sort();
    const inTests = BRIDGES.map((b) => b.name).sort();
    const inFixtures = Object.keys(FIXTURES).sort();
    expect(inTests).toEqual(registered);
    expect(inFixtures).toEqual(registered);
  });
});

describeBridge.each(BRIDGES)('bridge object: $name', ({ name, expected }) => {
  it('exposes the expected field set via the bridge meta', () => {
    expect(fieldNames(listBridgeFields(name))).toEqual(expected);
  });

  it('parses a fully-populated fixture without error', () => {
    const fixture = FIXTURES[name]();
    expect(() => parseBridge(name, fixture)).not.toThrow();
  });

  it('every field-getter and call-method round-trips successfully', () => {
    const fixture = FIXTURES[name]();
    expectAllInvocationsOk(invokeBridge(name, fixture));
  });
});

// Negative-required cases. Picked one representative bridge to keep the
// matrix above clean while still exercising the macro-generated
// `Field <X> is required` rejection path.
describeBridge('bridge object: try_new negative paths', () => {
  it('rejects a memberOrderBy fixture missing the required sql field', () => {
    expect(() => parseBridge('memberOrderBy', { dir: 'asc' })).toThrow(
      /Field sql is required/
    );
  });

  it('rejects a memberOrderBy fixture missing the required dir field', () => {
    expect(() => parseBridge('memberOrderBy', { sql: () => 'x' })).toThrow(
      /Field dir is required/
    );
  });
});

// Meta-shape assertion. Picked timeShiftDefinition because it has the most
// interesting mix: a trait `field` (sql) plus three serde-static fields,
// one of which is renamed via `#[serde(rename = "type")]`.
describeBridge('bridge object: meta shape', () => {
  it('reports js_name + kind for the serde-renamed static field on timeShiftDefinition', () => {
    const meta = listBridgeFields('timeShiftDefinition');
    const tsType = meta.find((m) => m.name === 'timeshift_type');
    expect(tsType?.jsName).toBe('type');
    expect(tsType?.kind).toBe('static');
    expect(tsType?.optional).toBe(true);
  });
});
