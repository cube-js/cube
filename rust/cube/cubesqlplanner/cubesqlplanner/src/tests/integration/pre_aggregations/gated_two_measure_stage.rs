//! Investigation of GitHub issue #11545: a multi_stage measure whose stage
//! SQL references two measures ("order_slice_gated") is reported to return
//! ~2x-inflated results for some months when a pre-aggregation covering both
//! referenced measures is used, depending on the surrounding date range.
//!
//! This reproduces the reported model shape (minus the MS SQL source, which
//! is irrelevant to the planner) as closely as possible:
//!   - `sale` and `ticket_fraction`: plain additive `sum` measures.
//!   - `order_slice_gated`: `multi_stage: true`, `type: number`, SQL is a
//!     `CASE WHEN {CUBE.sale} > 0 THEN {CUBE.ticket_fraction} END` — i.e. a
//!     Calculated measure referencing *two* base measures — with
//!     `add_group_by: [id]`.
//!   - `tickets`: `multi_stage: true`, `type: sum`, `sql: "{CUBE.order_slice_gated}"`.
//!   - One rollup covering `sale`, `ticket_fraction`, `id`, and
//!     `created_at` at `day` granularity.
//!
//! The test inspects the *generated SQL* (no live DB/CubeStore needed) for
//! queries over increasingly wide date ranges and prints it so the join
//! shape reported by the user (two rollup scans, LEFT/FULL JOINed on
//! `(id, month)`, `__usage_0`/`__usage_1`) can be checked by hand.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const YAML: &str = r#"
cubes:
    - name: orders
      sql: "SELECT * FROM ms_orders"
      dimensions:
          - name: id
            type: number
            sql: id
            primary_key: true
          - name: created_at
            type: time
            sql: created_at
      measures:
          - name: sale
            type: sum
            sql: sale

          - name: ticket_fraction
            type: sum
            sql: ticket_fraction

          - name: order_slice_gated
            type: number
            multi_stage: true
            sql: "CASE WHEN {CUBE.sale} > 0 THEN {CUBE.ticket_fraction} END"
            add_group_by:
                - id

          - name: tickets
            type: sum
            multi_stage: true
            sql: "{CUBE.order_slice_gated}"

      pre_aggregations:
          - name: orders_rollup
            type: rollup
            measures:
                - sale
                - ticket_fraction
            dimensions:
                - id
            time_dimension: created_at
            granularity: day
"#;

fn create_context() -> TestContext {
    let schema = MockSchema::from_yaml(YAML).unwrap();
    TestContext::new(schema).unwrap()
}

fn query_for_range(from: &str, to: &str) -> String {
    format!(
        indoc! {r#"
        measures:
          - orders.tickets
        time_dimensions:
          - dimension: orders.created_at
            granularity: month
            dateRange:
              - "{from}"
              - "{to}"
        order:
          - id: orders.created_at
    "#},
        from = from,
        to = to
    )
}

/// Dumps the generated SQL for a single-month vs. a multi-month range so the
/// two can be diffed by hand. Also asserts on the *usage count* and prints
/// which dimensions the FullKeyAggregate join between the two per-measure
/// leaves is keyed on.
#[tokio::test(flavor = "multi_thread")]
async fn test_gated_two_measure_stage_sql_shape() {
    let ctx = create_context();

    // Narrow range: single month.
    let narrow = query_for_range("2024-07-01", "2024-07-31");
    let (narrow_sql, narrow_usages) = ctx
        .build_sql_with_used_pre_aggregations(&narrow)
        .expect("narrow query should plan");

    // Wide range: six months, matching the issue's "Feb-Jul" shape.
    let wide = query_for_range("2024-02-01", "2024-07-31");
    let (wide_sql, wide_usages) = ctx
        .build_sql_with_used_pre_aggregations(&wide)
        .expect("wide query should plan");

    eprintln!("=== NARROW (single month) usages: {} ===", narrow_usages.len());
    for u in &narrow_usages {
        eprintln!(
            "  usage index={} name={} date_range={:?}",
            u.index,
            u.name(),
            u.date_range
        );
    }
    eprintln!("--- NARROW SQL ---\n{narrow_sql}\n");

    eprintln!("=== WIDE (six months) usages: {} ===", wide_usages.len());
    for u in &wide_usages {
        eprintln!(
            "  usage index={} name={} date_range={:?}",
            u.index,
            u.name(),
            u.date_range
        );
    }
    eprintln!("--- WIDE SQL ---\n{wide_sql}\n");

    // The whole point of the reported bug: the pre-aggregation is scanned
    // twice (once per measure referenced by the Calculate stage). Confirm
    // that shape exists, matching the reporter's __usage_0 / __usage_1
    // description.
    assert_eq!(
        narrow_usages.len(),
        2,
        "Expected 2 pre-aggregation usages (one per measure referenced by \
         order_slice_gated's Calculate stage); got {}",
        narrow_usages.len()
    );
    assert_eq!(
        wide_usages.len(),
        2,
        "Expected 2 pre-aggregation usages for the wide-range query too; got {}",
        wide_usages.len()
    );

    // Both usages must reference the same rollup.
    assert!(narrow_usages.iter().all(|u| u.name() == "orders_rollup"));
    assert!(wide_usages.iter().all(|u| u.name() == "orders_rollup"));

    if let Some(result) = ctx.try_execute(&narrow, "gated_two_measure_seed.sql").await {
        eprintln!("--- NARROW RESULT ---\n{result}");
    }
    if let Some(result) = ctx.try_execute(&wide, "gated_two_measure_seed.sql").await {
        eprintln!("--- WIDE RESULT ---\n{result}");
    }
}
