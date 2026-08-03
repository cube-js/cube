// Regression test for https://github.com/cube-js/cube/issues/11455
//
// Claim: when a cube's `primary_key` dimension is queried directly in
// `dimensions` and the query pulls measures from 2+ cubes (no shared join
// key -> full-key-aggregate join), the generated SQL's rejoin-to-source
// subquery (the "keys" derived table feeding the per-measure CTE) projects
// the primary key dimension twice under the same output alias (once as "the
// query dim", once as "the join key" used to re-fetch the measure from its
// own cube). Two columns sharing one alias inside a single derived table is
// itself ambiguous in Postgres: referencing that alias by table-qualified
// name from the enclosing join predicate (`"keys"."foo" = ...`) triggers
// `ERROR: column reference "foo" is ambiguous`, because the reference
// resolves to more than one attribute of "keys".
//
// Verified locally by running the resolved SQL against a real Postgres
// instance (not testcontainers - Docker wasn't available in the sandbox that
// authored this test): it reproduces
//   ERROR:  column reference "cube_a__id" is ambiguous
// exactly, confirming the reporter's claim mechanically.
//
// Investigation into the reporter's 3 preconditions:
//   1. "dimension is primary_key: true"   -> ESSENTIAL. See
//      `test_non_pk_dimension_does_not_reproduce` below: swapping the query
//      dimension for a non-PK dimension of the same cube removes the
//      duplicate alias entirely (different aliases: `cube_a__label` for the
//      query dim vs. `cube_a__id` for the rejoin key), and the bug vanishes.
//   2. "dimension is public: true"        -> NOT a real precondition of the
//      SQL-generation bug. `public`/`shown` never appears anywhere in the
//      Tesseract Rust planner (grep confirms zero references outside `pub
//      fn`); it is purely a JS-side metadata/API-visibility flag
//      (CubeToMetaTransformer::isVisible). See
//      `test_public_flag_does_not_affect_generated_sql` below: the generated
//      SQL is byte-for-byte identical whether or not `public: true` is set.
//      The reporter likely needed the field `public` just so their BI
//      tool/UI would let them pick it as a dimension in the first place -
//      that's a UX precondition for *triggering* the query, not a planner
//      precondition for the *bug*.
//   3. "measures from 2+ cubes"           -> ESSENTIAL. This is what forces
//      Tesseract into the full-key-aggregate join, which is what causes
//      cube_a's own measure to be recomputed via a rejoin ("keys" subquery)
//      instead of being read straight off a single-source CTE.
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/integration_pk_public_multi_source.yaml");
    TestContext::new(schema).unwrap()
}

const QUERY_PK_DIM: &str = indoc! {"
    measures:
      - cube_b.measure_b
      - cube_a.measure_a
    dimensions:
      - cube_a.id
    time_dimensions:
      - dimension: cube_b.date
        granularity: month
        dateRange:
          - \"2026-07-01\"
          - \"2026-07-31\"
"};

/// Finds every top-level `SELECT ... FROM` column list in `sql` (paren-depth
/// aware, so `date_trunc(...)` and nested subqueries don't confuse comma
/// splitting) and returns the trailing double-quoted output alias of each
/// comma-separated item, per SELECT clause.
fn select_list_aliases(sql: &str) -> Vec<Vec<String>> {
    let bytes = sql.as_bytes();
    let mut result = Vec::new();
    let mut i = 0;
    while let Some(rel) = sql[i..].find("SELECT") {
        let start = i + rel + "SELECT".len();
        // Find the matching top-level FROM for this SELECT (depth-aware).
        let mut depth = 0i32;
        let mut j = start;
        let mut from_pos = None;
        while j < bytes.len() {
            match bytes[j] {
                b'(' => depth += 1,
                b')' => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                _ => {
                    if depth == 0 && sql[j..].starts_with("FROM") {
                        from_pos = Some(j);
                        break;
                    }
                }
            }
            j += 1;
        }
        let Some(from_pos) = from_pos else {
            i = start;
            continue;
        };
        let clause = &sql[start..from_pos];
        let clause = clause.trim_start_matches(" DISTINCT").trim();

        // Split on top-level commas (depth-aware again for nested calls).
        let mut items = Vec::new();
        let mut item_start = 0;
        let mut depth = 0i32;
        let cb = clause.as_bytes();
        for (k, ch) in clause.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    items.push(&clause[item_start..k]);
                    item_start = k + 1;
                }
                _ => {}
            }
            let _ = cb;
        }
        items.push(&clause[item_start..]);

        let aliases: Vec<String> = items
            .iter()
            .filter_map(|item| {
                let item = item.trim();
                if item.is_empty() {
                    return None;
                }
                // Trailing token is the alias when it's a quoted identifier.
                if item.ends_with('"') {
                    let without_close = &item[..item.len() - 1];
                    without_close.rfind('"').map(|p| without_close[p + 1..].to_string())
                } else {
                    None
                }
            })
            .collect();

        result.push(aliases);
        i = from_pos + "FROM".len();
    }
    result
}

/// Asserts no SELECT clause in `sql` projects the same output alias twice -
/// which is what makes `"<derived_table>"."<alias>"` ambiguous when
/// referenced from an enclosing query, exactly like Postgres'
/// `column reference "..." is ambiguous`.
fn assert_no_duplicate_select_aliases(sql: &str) {
    for aliases in select_list_aliases(sql) {
        let mut seen = std::collections::HashSet::new();
        for alias in &aliases {
            assert!(
                seen.insert(alias.clone()),
                "duplicate output alias \"{}\" within a single SELECT list \
                 (aliases: {:?}) - this produces an ambiguous column \
                 reference in Postgres if referenced from an enclosing \
                 query. Full SQL:\n{}",
                alias,
                aliases,
                sql
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pk_dimension_with_multi_source_measures_is_ambiguous() {
    // This is a red (currently failing) regression test: it pins the bug
    // reported in cube-js/cube#11455. Once the planner dedupes the rejoin
    // key against an already-projected query dimension, this assertion will
    // pass and the test can be treated as a normal green regression test.
    let ctx = create_context();

    let sql = ctx.build_sql(QUERY_PK_DIM).unwrap();
    println!("{}", sql);

    assert!(
        sql.to_lowercase().contains("fk_aggregate_keys"),
        "expected a full-key-aggregate join (fk_aggregate_keys CTE) in:\n{}",
        sql
    );

    assert_no_duplicate_select_aliases(&sql);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_non_pk_dimension_does_not_reproduce() {
    // Same shape as the buggy query, but the queried dimension is a
    // non-primary-key dimension of cube_a (`label` instead of `id`). The
    // rejoin key added to fetch `cube_a.measure_a` is still `cube_a.id`, but
    // now it gets a *different* alias (`cube_a__id`) than the query
    // dimension's alias (`cube_a__label`), so there's no collision.
    let schema =
        MockSchema::from_yaml_file("common/integration_pk_public_multi_source_nonpk_dim.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let query = indoc! {"
        measures:
          - cube_b.measure_b
          - cube_a.measure_a
        dimensions:
          - cube_a.label
        time_dimensions:
          - dimension: cube_b.date
            granularity: month
            dateRange:
              - \"2026-07-01\"
              - \"2026-07-31\"
    "};

    let sql = ctx.build_sql(query).unwrap();
    println!("{}", sql);

    assert_no_duplicate_select_aliases(&sql);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_public_flag_does_not_affect_generated_sql() {
    // Same query/measures/dimensions as the buggy case, but the PK
    // dimension is NOT marked `public: true`. `public`/`shown` is a
    // metadata-visibility flag consumed only by the JS meta layer
    // (CubeToMetaTransformer::isVisible) - the Tesseract Rust planner never
    // reads it, so it cannot influence SQL generation. The generated SQL is
    // identical to the `public: true` case (bug and all), confirming that
    // precondition in the report is not load-bearing for the SQL-generation
    // defect - only for whether a UI happens to let a user pick that
    // dimension.
    let schema =
        MockSchema::from_yaml_file("common/integration_pk_public_multi_source_no_public.yaml");
    let ctx = TestContext::new(schema).unwrap();

    let sql_no_public = ctx.build_sql(QUERY_PK_DIM).unwrap();
    let sql_public = create_context().build_sql(QUERY_PK_DIM).unwrap();

    assert_eq!(
        sql_no_public, sql_public,
        "expected `public: true` to have zero effect on generated SQL"
    );
}
