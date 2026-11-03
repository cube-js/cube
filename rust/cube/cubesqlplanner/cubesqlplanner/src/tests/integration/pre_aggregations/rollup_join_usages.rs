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
const DATE_RANGE: (&str, &str) = ("2026-01-01T00:00:00.000", "2026-02-28T23:59:59.999");

const MULTI_STAGE_AND_PLAIN: &str = indoc! {"
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
"};

const NUMBER_OVER_BOTH: &str = indoc! {"
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
"};

fn served_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML)).unwrap()
}

fn source_ctx() -> TestContext {
    TestContext::new(MockSchema::from_yaml_file(YAML).only_pre_aggregations(&[])).unwrap()
}

fn leg_usage_indexes(source: &PreAggregationSource) -> Vec<Option<usize>> {
    match source {
        PreAggregationSource::Join(join) => std::iter::once(&join.root)
            .chain(join.items.iter().map(|item| &item.to))
            .flat_map(|leg| leg_usage_indexes(leg))
            .collect(),
        PreAggregationSource::Single(table) => vec![table.usage_index],
        PreAggregationSource::Union(union) => union
            .items
            .iter()
            .map(|item| item.table.usage_index)
            .collect(),
    }
}

fn assert_two_scans_of_the_join(sql: &str, usages: &[PreAggregationUsage]) {
    assert_eq!(usages.len(), 2, "expected two scans of the join:\n{sql}");
    for usage in usages {
        assert_eq!(usage.name(), "by_client", "{sql}");
        assert_eq!(
            leg_usage_indexes(usage.pre_aggregation.source()),
            vec![Some(usage.index); LEG_TABLES.len()],
            "every leg must belong to usage {}:\n{sql}",
            usage.index
        );
        for table in LEG_TABLES {
            let suffixed = format!("{}__usage_{} ", table, usage.index);
            assert!(sql.contains(&suffixed), "expected {suffixed} in:\n{sql}");
        }
        // The partitioned leg is cut per usage from this range.
        assert_eq!(
            usage.date_range,
            Some((DATE_RANGE.0.to_string(), DATE_RANGE.1.to_string())),
            "{sql}"
        );
    }
    TestContext::assert_pre_agg_tables_suffixed(sql, usages);
}

#[test]
fn test_rollup_join_scanned_twice_suffixes_every_leg() -> Result<(), CubeError> {
    let ctx = served_ctx();
    for query in [MULTI_STAGE_AND_PLAIN, NUMBER_OVER_BOTH] {
        let (sql, usages) = ctx.build_sql_with_used_pre_aggregations(query)?;
        assert_two_scans_of_the_join(&sql, &usages);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rollup_join_scanned_twice_agrees_with_source() -> Result<(), CubeError> {
    for query in [MULTI_STAGE_AND_PLAIN, NUMBER_OVER_BOTH] {
        let served = served_ctx().try_execute_pg(query, SEED).await;
        let source = source_ctx().try_execute_pg(query, SEED).await;
        assert_eq!(
            served, source,
            "rows served from the rollupJoin must match the source"
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
    for (name, query) in [
        ("multi_stage_and_plain", MULTI_STAGE_AND_PLAIN),
        ("number_over_both", NUMBER_OVER_BOTH),
    ] {
        if let Some(rows) = served_ctx().try_execute(query, SEED).await {
            insta::assert_snapshot!(name, rows);
        }
    }
    Ok(())
}
