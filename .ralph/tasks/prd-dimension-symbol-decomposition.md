# PRD: DimensionSymbol Decomposition

## Introduction

`DimensionSymbol` is a monolithic struct with 19+ fields serving all dimension types (string, number, boolean, time, geo, switch, case). Type-specific fields like `latitude`/`longitude` (geo only), `values` (switch only), `case` (case only) are stored as `Option` fields on every instance. The dimension type is a `String` compared via `== "geo"`, `== "time"`, `== "switch"` across 14+ locations.

This refactoring extracts type-specific data into separate structs (`RegularDimension`, `GeoDimension`, `SwitchDimension`, `CaseDimension`) wrapped in a `DimensionKind` enum, and introduces a `DimensionType` enum for scalar types. This makes invalid states unrepresentable and replaces string comparisons with type-safe pattern matching.

## Goals

- Extract dimension type-specific fields into dedicated structs wrapped in `DimensionKind` enum
- Introduce `DimensionType` enum (String, Number, Boolean, Time) replacing string comparisons
- Preserve all existing behavior — zero functional changes
- All existing tests must pass without modification
- Provide backward-compatible accessor methods to minimize call-site disruption
- Establish the pattern for future `MeasureSymbol` decomposition

## User Stories

### US-001: Create DimensionType enum
**Description:** As a developer, I need a type-safe enum for scalar dimension types so that string comparisons like `dimension_type() == "time"` are eliminated.

**Acceptance Criteria:**
- [ ] `DimensionType` enum with variants: `String`, `Number`, `Boolean`, `Time` in `symbols/common/dimension_type.rs`
- [ ] `from_str(&str) -> Result<Self, CubeError>` for conversion from definition strings
- [ ] `as_str() -> &'static str` for backward-compatible string output
- [ ] `symbols/common/mod.rs` updated to include and re-export the module
- [ ] Inline test for `from_str` / `as_str` round-trip and unknown type error
- [ ] `cargo check -p cubesqlplanner` passes

### US-002: Create RegularDimension struct
**Description:** As a developer, I need a struct for regular dimensions (string, number, boolean, time) that owns the `dimension_type` and `member_sql` fields.

**Acceptance Criteria:**
- [ ] `RegularDimension` struct in `symbols/dimension_kinds/regular.rs` with fields: `dimension_type: DimensionType`, `member_sql: Rc<SqlCall>`
- [ ] Methods: `dimension_type()`, `member_sql()`, `evaluate_sql(...)`, `get_dependencies(...)`, `get_dependencies_with_path(...)`, `apply_to_deps(...)`, `iter_sql_calls()`, `is_owned_by_cube()`
- [ ] `cargo check -p cubesqlplanner` passes

### US-003: Create GeoDimension struct
**Description:** As a developer, I need a struct for geo dimensions that owns the `latitude` and `longitude` SQL expressions.

**Acceptance Criteria:**
- [ ] `GeoDimension` struct in `symbols/dimension_kinds/geo.rs` with fields: `latitude: Rc<SqlCall>`, `longitude: Rc<SqlCall>`
- [ ] Methods: `latitude()`, `longitude()`, `evaluate_sql(...)`, `get_dependencies(...)`, `get_dependencies_with_path(...)`, `apply_to_deps(...)`, `iter_sql_calls()`, `is_owned_by_cube()`
- [ ] `cargo check -p cubesqlplanner` passes

### US-004: Create SwitchDimension struct
**Description:** As a developer, I need a struct for switch/calc-group dimensions that owns the `values` list and optional `member_sql`.

**Acceptance Criteria:**
- [ ] `SwitchDimension` struct in `symbols/dimension_kinds/switch.rs` with fields: `values: Vec<String>`, `member_sql: Option<Rc<SqlCall>>`
- [ ] Methods: `values()`, `member_sql()`, `is_calc_group()`, `evaluate_sql(...)`, `get_dependencies(...)`, `get_dependencies_with_path(...)`, `apply_to_deps(...)`, `iter_sql_calls()`, `is_owned_by_cube()`
- [ ] `is_calc_group()` returns `true` when `member_sql` is `None`
- [ ] `cargo check -p cubesqlplanner` passes

### US-005: Create CaseDimension struct
**Description:** As a developer, I need a struct for case dimensions that owns the `case` expression, scalar `dimension_type`, and optional `member_sql`.

**Acceptance Criteria:**
- [ ] `CaseDimension` struct in `symbols/dimension_kinds/case_dimension.rs` with fields: `dimension_type: DimensionType`, `case: Case`, `member_sql: Option<Rc<SqlCall>>`
- [ ] Methods: `dimension_type()`, `case()`, `member_sql()`, `replace_case(...)`, `evaluate_sql(...)`, `get_dependencies(...)`, `get_dependencies_with_path(...)`, `apply_to_deps(...)`, `iter_sql_calls()`, `is_owned_by_cube()`
- [ ] `cargo check -p cubesqlplanner` passes

### US-006: Create DimensionKind enum and module structure
**Description:** As a developer, I need the `DimensionKind` enum that wraps the four kind structs and delegates common operations.

**Acceptance Criteria:**
- [ ] `DimensionKind` enum in `symbols/dimension_kinds/mod.rs` with variants: `Regular(RegularDimension)`, `Geo(GeoDimension)`, `Switch(SwitchDimension)`, `Case(CaseDimension)`
- [ ] Delegation methods on enum: `evaluate_sql`, `get_dependencies`, `get_dependencies_with_path`, `apply_to_deps`, `iter_sql_calls`, `is_owned_by_cube`
- [ ] Convenience methods: `is_time()`, `is_geo()`, `is_switch()`, `is_case()`, `is_calc_group()`, `dimension_type_str() -> &str`
- [ ] `symbols/mod.rs` updated to include `dimension_kinds` module and re-exports
- [ ] `cargo check -p cubesqlplanner` passes

### US-007: Refactor DimensionSymbol to use DimensionKind
**Description:** As a developer, I need to replace the 6 type-specific flat fields in `DimensionSymbol` with a single `kind: DimensionKind` field.

**Acceptance Criteria:**
- [ ] Remove fields from `DimensionSymbol`: `dimension_type`, `member_sql`, `latitude`, `longitude`, `values`, `case`
- [ ] Add field: `kind: DimensionKind`
- [ ] Constructor receives `kind: DimensionKind` instead of 6 separate parameters
- [ ] Internal methods (`evaluate_sql`, `get_dependencies`, `get_dependencies_with_path`, `apply_to_deps`, `iter_sql_calls`, `DebugSql`) delegate to `self.kind`
- [ ] Backward-compatible accessors preserved: `dimension_type() -> &str`, `member_sql() -> Option<&Rc<SqlCall>>`, `latitude()`, `longitude()`, `values() -> &[String]`, `case() -> Option<&Case>`
- [ ] New accessors added: `kind()`, `is_time()`, `is_geo()`, `is_switch()`, `is_case()`
- [ ] `replace_case` works on Case variant
- [ ] `cargo check -p cubesqlplanner` passes
- [ ] **All existing tests pass without modification** (`cargo test -p cubesqlplanner`)

### US-008: Refactor DimensionSymbolFactory to build DimensionKind
**Description:** As a developer, I need the factory to construct the appropriate `DimensionKind` variant based on the dimension definition.

**Acceptance Criteria:**
- [ ] Factory builds: `CaseDimension` when case present, `GeoDimension` when type is "geo", `SwitchDimension` when type is "switch", `RegularDimension` otherwise
- [ ] `owned_by_cube` computed via `kind.is_owned_by_cube()`
- [ ] `is_reference` logic uses `kind.is_case()` / `kind.is_geo()` instead of checking `Option` fields
- [ ] `cargo check -p cubesqlplanner` passes
- [ ] **All existing tests pass without modification** (`cargo test -p cubesqlplanner`)

### US-009: Replace string comparisons at call sites
**Description:** As a developer, I want to replace `dimension_type() == "geo"/"time"/"switch"` string comparisons with type-safe enum methods.

**Acceptance Criteria:**
- [ ] `sql_nodes/geo_dimension.rs`: `dimension_type() == "geo"` replaced with `is_geo()`
- [ ] `sql_nodes/time_shift.rs`: `dimension_type() == "time"` replaced with `is_time()`
- [ ] `symbols/time_dimension_symbol.rs`: `dimension_type() == "time"` replaced with `is_time()`
- [ ] `sql_call_builder.rs`: `dimension_type() == "time"` replaced with `is_time()`
- [ ] `symbols/common/case.rs`: `dimension_type() == "switch"` replaced with `is_switch()`
- [ ] `symbols/common/static_filter.rs`: `dimension_type() == "switch"` replaced with `is_switch()`
- [ ] `planners/multi_stage/applied_state.rs`: `dimension_type() == "time"` replaced with `is_time()`
- [ ] `logical_plan/optimizers/pre_aggregation/pre_aggregations_compiler.rs`: `dimension_type() != "time"` replaced with `!is_time()`
- [ ] No existing tests modified
- [ ] `cargo test -p cubesqlplanner` passes
- [ ] `cargo clippy -p cubesqlplanner` passes

## Functional Requirements

- FR-1: `DimensionType` enum must support variants: `String`, `Number`, `Boolean`, `Time`
- FR-2: `DimensionType::from_str` must return `CubeError` for unknown types (not "geo", not "switch" — those are structural, not scalar)
- FR-3: `DimensionKind::Regular` always has `member_sql` (non-optional)
- FR-4: `DimensionKind::Geo` always has both `latitude` and `longitude` (non-optional)
- FR-5: `DimensionKind::Switch` has `member_sql: Option` — `None` means calc_group
- FR-6: `DimensionKind::Case` has `dimension_type: DimensionType` for the scalar return type of the case expression
- FR-7: Backward-compatible `dimension_type() -> &str` returns `"geo"` for Geo, `"switch"` for Switch, and `dim_type.as_str()` for Regular/Case
- FR-8: `member_sql()` return type changes from `&Option<Rc<SqlCall>>` to `Option<&Rc<SqlCall>>`
- FR-9: `values()` return type changes from `&Vec<String>` to `&[String]`, returning `&[]` for non-Switch
- FR-10: Each kind struct owns its `evaluate_sql`, `get_dependencies`, `get_dependencies_with_path`, `apply_to_deps`, `iter_sql_calls`, `is_owned_by_cube` implementations
- FR-11: `DimensionKind` delegates all operations to inner struct via match
- FR-12: If any existing test requires modification, implementation must stop and report the issue

## Non-Goals

- No changes to `MeasureSymbol` (future work, same pattern)
- No changes to time-shift fields (`time_shift`, `time_shift_pk_full_name`, `is_self_time_shift_pk`) — multi-stage concern
- No changes to sub-query fields (`is_sub_query`, `propagate_filters_to_sub_query`) — multi-stage sub-variant
- No changes to `base_filter.rs` (reads `dimension_type` from bridge definition, not `DimensionSymbol`)
- No changes to test fixtures / mocks — string-to-enum conversion happens in factory
- No changes to `member_symbol.rs` — delegates to `DimensionSymbol`, API compatible
- No new trait abstractions or trait objects for dimension kinds

## Technical Considerations

- All kind structs must implement `Clone` (used by `apply_to_deps` which clones `self`)
- `Rc<SqlCall>` is used everywhere — kind structs store `Rc<SqlCall>`, not owned `SqlCall`
- Factory priority for kind selection: Case > Geo > Switch > Regular (case is checked first because a dimension with a case expression has a structurally different evaluation)
- `apply_to_deps` on `DimensionSymbol` clones self, replaces `kind` with transformed version, returns `MemberSymbol::new_dimension(Rc::new(result))`
- Files not requiring changes: `calc_group_dims_collector.rs` (already uses `is_calc_group()`), `sub_query_dimensions.rs`, `join_hints_collector.rs`, `find_owned_by_cube.rs` (don't use `dimension_type`)

### File organization

```
symbols/
  common/
    mod.rs              (existing, add dimension_type module)
    case.rs             (existing, minor: "switch" string comparison -> is_switch())
    static_filter.rs    (existing, minor: "switch" string comparison -> is_switch())
    symbol_path.rs      (existing, no changes)
    dimension_type.rs   (NEW)
  dimension_kinds/
    mod.rs              (NEW — DimensionKind enum + re-exports)
    regular.rs          (NEW — RegularDimension)
    geo.rs              (NEW — GeoDimension)
    switch.rs           (NEW — SwitchDimension)
    case_dimension.rs   (NEW — CaseDimension)
  dimension_symbol.rs   (REFACTORED)
  mod.rs                (add dimension_kinds module)
```

## Success Metrics

- All existing tests pass (`cargo test -p cubesqlplanner`)
- Zero string comparisons for dimension type remain in production code
- `cargo clippy -p cubesqlplanner` passes
- No `Option` fields in `DimensionSymbol` that are type-specific (latitude, longitude, values, case removed)
- Invalid states (e.g., geo dimension without latitude) are unrepresentable at type level

## Open Questions

- Should backward-compatible accessors (`dimension_type() -> &str`) be marked for future removal or kept permanently? Current decision: keep as convenient shortcuts alongside enum API.
