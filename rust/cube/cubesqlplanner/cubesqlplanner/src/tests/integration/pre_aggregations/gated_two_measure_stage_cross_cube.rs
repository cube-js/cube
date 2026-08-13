//! Variant of `gated_two_measure_stage` matching the issue's two-cube shape
//! more closely: `order_slice_gated` and `tickets` live on the *order*
//! dimension cube, joined many_to_one from the `fact` cube, and the gate
//! references two measures on `fact` directly across that join. The rollup
//! is declared on `fact` and covers `sale`, `ticket_fraction`, `orders.id`,
//! and `fact.created_at` at day granularity — as described in the issue.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;

const YAML: &str = r#"
cubes:
    - name: fact
      sql: "SELECT * FROM ms_fact"
      joins:
          - name: orders
            relationship: many_to_one
            sql: "{fact}.order_id = {orders.id}"
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

      pre_aggregations:
          - name: fact_rollup
            type: rollup
            measures:
                - sale
                - ticket_fraction
            dimensions:
                - orders.id
            time_dimension: created_at
            granularity: day

    - name: orders
      sql: "SELECT * FROM ms_orders"
      dimensions:
          - name: id
            type: number
            sql: id
            primary_key: true

      measures:
          - name: order_slice_gated
            type: number
            multi_stage: true
            sql: "CASE WHEN {fact.sale} > 0 THEN {fact.ticket_fraction} END"
            add_group_by:
                - id

          - name: tickets
            type: sum
            multi_stage: true
            sql: "{CUBE.order_slice_gated}"
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
          - dimension: fact.created_at
            granularity: month
            dateRange:
              - "{from}"
              - "{to}"
        order:
          - id: fact.created_at
    "#},
        from = from,
        to = to
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_gated_two_measure_stage_cross_cube_sql_shape() {
    let ctx = create_context();

    let narrow = query_for_range("2024-07-01", "2024-07-31");
    let narrow_res = ctx.build_sql_with_used_pre_aggregations(&narrow);

    let wide = query_for_range("2024-02-01", "2024-07-31");
    let wide_res = ctx.build_sql_with_used_pre_aggregations(&wide);

    match &narrow_res {
        Ok((sql, usages)) => {
            eprintln!("=== NARROW usages: {} ===", usages.len());
            for u in usages {
                eprintln!("  index={} name={} date_range={:?}", u.index, u.name(), u.date_range);
            }
            eprintln!("--- NARROW SQL ---\n{sql}\n");
        }
        Err(e) => eprintln!("NARROW build_sql failed: {e:?}"),
    }

    match &wide_res {
        Ok((sql, usages)) => {
            eprintln!("=== WIDE usages: {} ===", usages.len());
            for u in usages {
                eprintln!("  index={} name={} date_range={:?}", u.index, u.name(), u.date_range);
            }
            eprintln!("--- WIDE SQL ---\n{sql}\n");
        }
        Err(e) => eprintln!("WIDE build_sql failed: {e:?}"),
    }
}
