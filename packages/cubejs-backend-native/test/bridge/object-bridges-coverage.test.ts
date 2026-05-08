import {
  bridgeHarnessAvailable,
  buildFixture,
  fieldNames,
  listBridgeFields,
  parseBridge,
} from './helpers';

// Table-driven coverage for every bridge that uses #[nativebridge::native_bridge].
// Each entry pins:
//   - `expected`: full set of field names (trait + static), keyed by the
//     Rust ident (snake_case). `fieldNames(...)` returns the same shape.
//   - `overrides`: serde-driven static fields that need a typed value
//     (non-Option primitives) so `from_native` does not reject the fixture.
//     Keys here are jsName (camelCase, post-`#[serde(rename)]`) — the same
//     shape JS sees on the wire — NOT the Rust ident from `expected`.
//
// Naming dualism: `expected` is Rust-side, `overrides` is JS-side. They line
// up via `BridgeFieldMeta { name, js_name }`, surfaced through `fieldNames`
// and `buildFixture` respectively.
//
// Fully-populated fixture parsing is the smoke test for try_new: required
// trait fields get auto-stub `() => null`, optional fields are omitted, and
// static overrides are merged in. NB: try_new only verifies that each
// required field is *present* on the JS object via `has_field` — it does
// not call functions or validate signatures. So for bridges with `kind:
// 'call'` methods (e.g. cubeEvaluator, baseTools, driverTools), parseBridge
// here is an existence smoke test, not a behavior test. Behavior coverage
// lives in higher-level integration tests.
//
// Coverage scope: every trait annotated with #[nativebridge::native_bridge]
// is registered in `bridge_registry!` and pinned by a row below. Hand-rolled
// bridges (MemberSql, FilterParamsCallback, SqlTemplatesRender) live
// outside the macro and are not in scope here.
type BridgeSpec = {
  name: string;
  expected: string[];
  overrides?: Record<string, unknown>;
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
    overrides: {
      exportAnnotatedSql: false,
      disableExternalPreAggregations: false,
    },
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
  {
    name: 'caseDefinition',
    expected: ['else_label', 'when'],
  },
  {
    name: 'caseElseItem',
    expected: ['label'],
  },
  {
    name: 'caseItem',
    expected: ['label', 'sql'],
  },
  {
    name: 'caseSwitchDefinition',
    expected: ['else_sql', 'switch', 'when'],
  },
  {
    name: 'caseSwitchElseItem',
    expected: ['sql'],
  },
  {
    name: 'caseSwitchItem',
    expected: ['sql', 'value'],
    overrides: { value: '' },
  },
  {
    name: 'cubeDefinition',
    expected: ['is_calendar', 'is_view', 'join_map', 'name', 'sql', 'sql_alias', 'sql_table'],
    overrides: { name: '' },
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
    overrides: { primaryKeys: {} },
  },
  {
    name: 'dimensionDefinition',
    expected: [
      'add_group_by_references',
      'case',
      'dimension_type',
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
    overrides: { type: '' },
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
    overrides: { type: '' },
  },
  {
    name: 'filterGroup',
    expected: [],
  },
  {
    name: 'filterParams',
    expected: [],
  },
  {
    name: 'geoItem',
    expected: ['sql'],
  },
  {
    name: 'granularityDefinition',
    expected: ['interval', 'offset', 'origin', 'sql'],
    overrides: { interval: '' },
  },
  {
    name: 'joinDefinition',
    expected: ['joins', 'multiplication_factor', 'root'],
    overrides: { root: '', multiplicationFactor: {} },
  },
  {
    name: 'joinGraph',
    expected: ['build_join'],
  },
  {
    name: 'joinItem',
    expected: ['from', 'join', 'original_from', 'original_to', 'to'],
    overrides: { from: '', to: '', originalFrom: '', originalTo: '' },
  },
  {
    name: 'joinItemDefinition',
    expected: ['relationship', 'sql'],
    overrides: { relationship: '' },
  },
  {
    name: 'measureDefinition',
    expected: [
      'add_group_by_references',
      'case',
      'drill_filters',
      'filters',
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
    overrides: { type: '' },
  },
  {
    name: 'memberDefinition',
    expected: ['member_type', 'sql'],
    overrides: { type: '' },
  },
  {
    name: 'memberExpressionDefinition',
    expected: ['cube_name', 'definition', 'expression', 'expression_name', 'name'],
  },
  {
    name: 'memberOrderBy',
    expected: ['dir', 'sql'],
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
    overrides: { name: '', type: '' },
  },
  {
    name: 'preAggregationObj',
    expected: ['cube', 'pre_aggregation_id', 'pre_aggregation_name', 'table_name'],
  },
  {
    name: 'preAggregationTimeDimension',
    expected: ['dimension', 'granularity'],
    overrides: { granularity: '' },
  },
  {
    name: 'securityContext',
    expected: [],
  },
  {
    name: 'segmentDefinition',
    expected: ['owned_by_cube', 'segment_type', 'sql'],
  },
  {
    name: 'sqlUtils',
    expected: [],
  },
  {
    name: 'structWithSqlMember',
    expected: ['sql'],
  },
  {
    name: 'timeShiftDefinition',
    expected: ['interval', 'name', 'sql', 'timeshift_type'],
  },
];

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge.each(BRIDGES)('bridge object: $name', ({ name, expected, overrides }) => {
  it('exposes the expected field set via the bridge meta', () => {
    expect(fieldNames(listBridgeFields(name))).toEqual(expected);
  });

  it('parses a fully-populated fixture without error', () => {
    const fixture = buildFixture(listBridgeFields(name), overrides ?? {});
    expect(() => parseBridge(name, fixture)).not.toThrow();
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

// Self-test for the helper. A misspelled override key should fail loudly,
// not silently cascade into a confusing serde error.
describeBridge('helpers: buildFixture', () => {
  it('throws on an unknown override key (typo guard)', () => {
    const meta = listBridgeFields('memberOrderBy');
    expect(() => buildFixture(meta, { dirr: 'x' })).toThrow(
      /override key 'dirr'/
    );
  });
});
