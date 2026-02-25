# PRD: MeasureKind Type Decomposition

## Introduction

Replace string-based measure type comparisons in MeasureSymbol with a structured `MeasureKind` enum, following the pattern established by `DimensionKind`. Currently measure types are stored as `String` and compared via string literals scattered across 10+ files. This creates fragile, error-prone code where typos compile silently and adding a new type requires hunting down all comparison sites.

## Goals

- Encode measure type invariants at the type level (e.g., count's optional SQL)
- Eliminate string-based measure type comparisons in MeasureSymbol
- Provide a flat `AggregationType` enum with behavioral methods (`is_additive()`, `sql_function_name()`)
- Maintain consistency with the DimensionKind decomposition pattern
- Add missing sql_evaluator test coverage for untested measure types

## Design

### Type Hierarchy

```
MeasureKind
├── Count(CountMeasure)              — structurally unique (optional SQL, pk_sqls)
├── Aggregated(AggregatedMeasure)    — sum/avg/min/max/countDistinct/countDistinctApprox/numberAgg
├── Calculated(CalculatedMeasure)    — number/string/time/boolean
└── Unknown(String)                  — fallback for types not yet decomposed (runningTotal, rank, etc.)

AggregationType (flat enum inside AggregatedMeasure)
├── Sum
├── Avg
├── Min
├── Max
├── CountDistinct
├── CountDistinctApprox
└── NumberAgg
```

### Multi-stage Types (runningTotal, rank) — Out of Scope

RunningTotal and Rank are multi-stage measure types with complex behavior that requires separate design work. They remain as `MeasureKind::Unknown(String)` for now, preserving the current string-based handling. A dedicated PRD for multi-stage measure decomposition will follow.

### Why Count is a Separate Struct

Count is the only measure type where:
1. `member_sql` is optional (`COUNT(*)` needs no SQL expression)
2. When SQL is absent, `pk_sqls` are used for join multiplication handling
3. `owned_by_cube` is always true when SQL is absent
4. Can be promoted to `countDistinct` in multiplied join contexts

```rust
pub enum CountSql {
    Auto(Vec<Rc<SqlCall>>),  // COUNT(*), pk_sqls for join handling
    Explicit(Rc<SqlCall>),    // COUNT(expression)
}

pub struct CountMeasure {
    sql: CountSql,
}
```

### Why Other Aggregates Stay as Enum

sum/avg/min/max/countDistinct/countDistinctApprox/numberAgg all carry identical data (`member_sql: Rc<SqlCall>`). Their differences are purely behavioral (which SQL function to generate). Separate structs would be empty wrappers over the same field — over-engineering.

### Case Remains Orthogonal

Unlike `DimensionKind::Case` where case *replaces* dimension behavior, measure `case` works *alongside* the type. In practice, case is used with `type: number` + `multi_stage: true`. Case stays as `Option<Case>` on MeasureSymbol.

## User Stories

### US-001: Define AggregationType Enum

**Description:** As a developer, I want a typed enum for aggregation functions so that behavioral checks like `is_additive()` are centralized and exhaustive.

**Acceptance Criteria:**
- [ ] Create `AggregationType` enum with variants: `Sum`, `Avg`, `Min`, `Max`, `CountDistinct`, `CountDistinctApprox`, `NumberAgg`
- [ ] Implement `is_additive(&self) -> bool` — true for Sum, Min, Max
- [ ] Implement `is_distinct(&self) -> bool` — true for CountDistinct, CountDistinctApprox
- [ ] Implement `sql_function_name(&self) -> &str` — returns "sum", "avg", etc.
- [ ] Implement `From<&str>` / `TryFrom<&str>` for parsing from definition strings (handle both camelCase "countDistinct" and snake_case "count_distinct")
- [ ] Existing tests pass (`cargo test`)

### US-002: Define CountMeasure Struct

**Description:** As a developer, I want a `CountMeasure` struct that encodes the count-specific invariant of optional SQL with pk_sqls fallback.

**Acceptance Criteria:**
- [ ] Create `CountSql` enum: `Auto(Vec<Rc<SqlCall>>)` and `Explicit(Rc<SqlCall>)`
- [ ] Create `CountMeasure` struct with `sql: CountSql`
- [ ] Implement standard methods: `evaluate_sql()`, `get_dependencies()`, `get_dependencies_with_path()`, `apply_to_deps()`, `iter_sql_calls()`, `is_owned_by_cube()`
- [ ] `is_owned_by_cube()` returns true for `CountSql::Auto`
- [ ] Existing tests pass (`cargo test`)

### US-003: Define AggregatedMeasure Struct

**Description:** As a developer, I want an `AggregatedMeasure` struct that pairs `AggregationType` with a required `member_sql`.

**Acceptance Criteria:**
- [ ] Create `AggregatedMeasure` struct with `agg_type: AggregationType` and `member_sql: Rc<SqlCall>`
- [ ] Implement standard methods: `evaluate_sql()`, `get_dependencies()`, `get_dependencies_with_path()`, `apply_to_deps()`, `iter_sql_calls()`, `is_owned_by_cube()`
- [ ] Existing tests pass (`cargo test`)

### US-004: Define CalculatedMeasure Struct

**Description:** As a developer, I want a `CalculatedMeasure` struct for number/string/time/boolean measures that reference other measures.

**Acceptance Criteria:**
- [ ] Create `CalculatedMeasureType` enum: `Number`, `String`, `Time`, `Boolean`
- [ ] Create `CalculatedMeasure` struct with `calc_type: CalculatedMeasureType` and `member_sql: Rc<SqlCall>`
- [ ] Implement standard methods: `evaluate_sql()`, `get_dependencies()`, `get_dependencies_with_path()`, `apply_to_deps()`, `iter_sql_calls()`, `is_owned_by_cube()`
- [ ] Existing tests pass (`cargo test`)

### US-005: Define MeasureKind Enum and Integrate into MeasureSymbol

**Description:** As a developer, I want a top-level `MeasureKind` enum that replaces `measure_type: String` in `MeasureSymbol`, providing type-safe access to measure behavior.

**Acceptance Criteria:**
- [ ] Create `MeasureKind` enum with variants: `Count(CountMeasure)`, `Aggregated(AggregatedMeasure)`, `Calculated(CalculatedMeasure)`, `Unknown(String)`
- [ ] Implement delegating methods on `MeasureKind`: `evaluate_sql()`, `get_dependencies()`, `get_dependencies_with_path()`, `apply_to_deps()`, `iter_sql_calls()`, `is_owned_by_cube()`
- [ ] Implement type check methods: `is_calculated()`, `is_additive()`, `measure_type_str()`
- [ ] `Unknown(String)` variant handles runningTotal, rank, and any future types not yet decomposed — delegates to string-based logic
- [ ] Replace `measure_type: String` field in `MeasureSymbol` with `kind: MeasureKind`
- [ ] Update `MeasureSymbolFactory::build()` to construct appropriate `MeasureKind` variant
- [ ] Update `MeasureSymbol` methods to delegate to `MeasureKind`
- [ ] Keep backward-compatible `measure_type() -> &str` method via `kind.measure_type_str()`
- [ ] `case: Option<Case>` remains an orthogonal field on `MeasureSymbol`
- [ ] All existing tests pass (`cargo test`)

### US-006: Add Missing sql_evaluator Tests for Measure Types

**Description:** As a developer, I want sql_evaluator test coverage for all measure types so that the MeasureKind refactoring is verified.

Note: Only add tests for types that don't already have coverage. Run existing tests first to confirm they pass before adding new ones.

**Already covered:** count, sum, min, max, avg, countDistinct, countDistinctApprox, number

**Missing coverage:** string, time, boolean, numberAgg

**Acceptance Criteria:**
- [ ] Existing tests pass before any additions (`cargo test`)
- [ ] Add test schema YAML with definitions for missing measure types (string, time, boolean, numberAgg)
- [ ] Add `string_measure()` test in symbol_evaluator — verifies SQL evaluation for string calculated type
- [ ] Add `time_measure()` test in symbol_evaluator — verifies SQL evaluation for time calculated type
- [ ] Add `boolean_measure()` test in symbol_evaluator — verifies SQL evaluation for boolean calculated type
- [ ] Add `number_agg_measure()` test in symbol_evaluator — verifies SQL evaluation for numberAgg type
- [ ] All new tests pass (`cargo test`)

## Functional Requirements

- FR-1: `AggregationType` enum must support parsing from both camelCase (`countDistinct`) and snake_case (`count_distinct`) strings
- FR-2: `CountMeasure` must distinguish between auto-count (no SQL, uses pk_sqls) and explicit-count (has SQL expression)
- FR-3: `MeasureKind` must provide a `measure_type_str()` method returning the original string representation for backward compatibility with sql_nodes
- FR-4: `MeasureSymbol::new_patched()` must work with `MeasureKind`, translating string-based type replacements to kind transformations
- FR-5: `MeasureSymbol::new_unrolling()` must correctly transform kind when removing rolling window
- FR-6: `case: Option<Case>` must remain an independent field on `MeasureSymbol`, not part of `MeasureKind`
- FR-7: All methods currently on `MeasureSymbol` that check `measure_type` string must be expressible through `MeasureKind`

## Non-Goals

- Refactoring sql_nodes consumers (FinalMeasureSqlNode, RollingWindowNode, etc.) — follow-up PR
- Refactoring string comparisons in planners, collectors, and other consumers — follow-up PR
- Removing `measure_type_str()` backward compatibility — only after all consumers migrate
- Changing MeasureDefinition trait or cube_bridge layer
- Adding new measure type support beyond what exists today
- Decomposing multi-stage types (runningTotal, rank) — separate PRD

## Technical Considerations

- File structure: `src/planner/sql_evaluator/symbols/measure_kinds/` mirroring `dimension_kinds/`
  - `mod.rs` — MeasureKind enum definition
  - `count.rs` — CountMeasure, CountSql
  - `aggregated.rs` — AggregatedMeasure, AggregationType
  - `calculated.rs` — CalculatedMeasure, CalculatedMeasureType
- Each struct implements the same method set as DimensionKind variants: `evaluate_sql()`, `get_dependencies()`, `get_dependencies_with_path()`, `apply_to_deps()`, `iter_sql_calls()`, `is_owned_by_cube()`
- `new_patched()` needs careful handling: type replacement validation moves from string matching to kind-aware logic
- Tests go in `src/tests/`, test schemas in `src/test_fixtures/schemas/yaml_files/symbol_evaluator/`

## Success Metrics

- Zero string-based measure type comparisons remaining in MeasureSymbol
- All existing tests pass without modification
- New tests cover 4 previously untested measure types (string, time, boolean, numberAgg)
- `cargo clippy` passes

## Open Questions

- Should `new_patched()` accept `AggregationType` instead of `String` for the replacement type, or keep string input for now (since callers are not yet refactored)?
- Should `CalculatedMeasureType` reuse `DimensionType` enum (String, Number, Time, Boolean) or be a separate type?
