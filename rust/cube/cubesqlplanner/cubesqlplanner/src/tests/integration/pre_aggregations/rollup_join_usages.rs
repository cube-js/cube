//! A rollupJoin scanned more than once in one query: each scan is its own usage, and every
//! rollup the join reads has to be addressed under that usage's `__usage_N` name.

use crate::logical_plan::{PreAggregationSource, PreAggregationUsage};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use cubenativeutils::CubeError;
use indoc::indoc;

const YAML: &str = "common/rollup_join_multi_usage.yaml";
const SEED: &str = "rollup_join_multi_usage_tables.sql";
const LEG_TABLES: [&str; 3] = [
    "delivery__by_tactic_day",
    "tactics__brands",
    "client_mapping__by_brand",
];
const JAN_FEB: (&str, &str) = ("2026-01-01T00:00:00.000", "2026-02-28T23:59:59.999");
const FEB_MAR: (&str, &str) = ("2026-02-01T00:00:00.000", "2026-03-31T23:59:59.999");

struct Case {
    name: &'static str,
    query: &'static str,
    /// The range each usage reads, so the partitioned leg is cut per usage.
    usage_ranges: [(&'static str, &'static str); 2],
}

const CASES: [Case; 4] = [
    Case {
        name: "multi_stage_and_plain",
        query: indoc! {"
            measures:
              - delivery.gross_spend
              - delivery.impressions
            dimensions:
              - client_mapping.client_id
            time_dimensions:
              - dimension: delivery.day
                dateRange:
                  - '2026-01-01'
                  - '2026-02-28'
            order:
              - id: client_mapping.client_id
        "},
        usage_ranges: [JAN_FEB, JAN_FEB],
    },
    Case {
        name: "number_over_both",
        query: indoc! {"
            measures:
              - delivery.cpm
            dimensions:
              - client_mapping.client_id
            time_dimensions:
              - dimension: delivery.day
                dateRange:
                  - '2026-01-01'
                  - '2026-02-28'
            order:
              - id: client_mapping.client_id
        "},
        usage_ranges: [JAN_FEB, JAN_FEB],
    },
    Case {
        name: "two_multi_stage",
        query: indoc! {"
            measures:
              - delivery.gross_spend
              - delivery.impressions_staged
            dimensions:
              - client_mapping.client_id
            time_dimensions:
              - dimension: delivery.day
                dateRange:
                  - '2026-01-01'
                  - '2026-02-28'
            order:
              - id: client_mapping.client_id
        "},
        usage_ranges: [JAN_FEB, JAN_FEB],
    },
    Case {
        name: "shifted_and_plain",
        query: indoc! {"
            measures:
              - delivery.impressions
              - delivery.impressions_prev_month
            dimensions:
              - client_mapping.client_id
            time_dimensions:
              - dimension: delivery.day
                granularity: month
                dateRange:
                  - '2026-02-01'
                  - '2026-03-31'
            order:
              - id: client_mapping.client_id
              - id: delivery.day
        "},
        usage_ranges: [JAN_FEB, FEB_MAR],
    },
];

fn served_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML)).unwrap()
}

fn source_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[])).unwrap()
}

fn leg_usage_indexes(source: &PreAggregationSource) -> Vec<Option<usize>> {
    TestContext::collect_pre_agg_source_tables(source)
        .iter()
        .map(|table| table.usage_index)
        .collect()
}

fn assert_two_scans_of_the_join(case: &Case, sql: &str, usages: &[PreAggregationUsage]) {
    assert_eq!(
        usages.len(),
        2,
        "{}: expected two scans of the join:\n{sql}",
        case.name
    );
    for usage in usages {
        assert_eq!(usage.name(), "by_client", "{}:\n{sql}", case.name);
        assert_eq!(
            leg_usage_indexes(usage.pre_aggregation.source()),
            vec![Some(usage.index); LEG_TABLES.len()],
            "{}: every leg must belong to usage {}:\n{sql}",
            case.name,
            usage.index
        );
        for table in LEG_TABLES {
            let suffixed = format!("{}__usage_{} ", table, usage.index);
            assert!(
                sql.contains(&suffixed),
                "{}: expected {suffixed} in:\n{sql}",
                case.name
            );
        }
    }
    let mut ranges = usages
        .iter()
        .map(|usage| usage.date_range.clone())
        .collect::<Vec<_>>();
    ranges.sort();
    let expected = case
        .usage_ranges
        .iter()
        .map(|(from, to)| Some((from.to_string(), to.to_string())))
        .collect::<Vec<_>>();
    assert_eq!(ranges, expected, "{}:\n{sql}", case.name);
    TestContext::assert_pre_agg_tables_suffixed(sql, usages);
}

#[test]
fn test_rollup_join_scanned_twice_suffixes_every_leg() -> Result<(), CubeError> {
    let ctx = served_ctx();
    for case in &CASES {
        let (sql, usages) = ctx.build_sql_with_used_pre_aggregations(case.query)?;
        assert_two_scans_of_the_join(case, &sql, &usages);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rollup_join_scanned_twice_agrees_with_source() -> Result<(), CubeError> {
    for case in &CASES {
        let served = served_ctx().try_execute_pg(case.query, SEED).await;
        let source = source_ctx().try_execute_pg(case.query, SEED).await;
        assert_eq!(
            served, source,
            "{}: rows served from the rollupJoin must match the source",
            case.name
        );
        #[cfg(feature = "integration-postgres")]
        assert!(
            served.is_some(),
            "Postgres execution is enabled but returned no result"
        );
    }
    Ok(())
}

// Cube Store is where a bare leg name fails: it has no table under that name.
#[tokio::test(flavor = "multi_thread")]
async fn test_rollup_join_scanned_twice_runs_on_cubestore() -> Result<(), CubeError> {
    for case in &CASES {
        if let Some(rows) = served_ctx().try_execute(case.query, SEED).await {
            insta::assert_snapshot!(case.name, rows);
        }
    }
    Ok(())
}
