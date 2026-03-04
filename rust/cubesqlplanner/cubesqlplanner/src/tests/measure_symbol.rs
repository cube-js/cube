//! Tests for MeasureSymbol: kind classification, new_patched, and helper methods

use crate::planner::sql_evaluator::{
    AggregationType, CalculatedMeasureType, MeasureKind, SqlCall,
};
use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use std::rc::Rc;

fn ctx() -> TestContext {
    let schema = MockSchema::from_yaml_file("common/measure_kind_tests.yaml");
    TestContext::new(schema).unwrap()
}

fn get_filter_calls(ctx: &TestContext) -> Vec<Rc<SqlCall>> {
    let symbol = ctx.create_measure("test_measures.filtered_total").unwrap();
    symbol.as_measure().unwrap().measure_filters().clone()
}

// ─── Per-measure property tests ─────────────────────────────────────────────

#[test]
fn measure_count_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.cnt").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(measure.kind(), MeasureKind::Count(_)));
    assert!(!measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_rolling_window());
    assert!(!measure.is_cumulative());
    assert!(measure.is_addictive());
}

#[test]
fn measure_sum_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.total").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::Sum
    ));
    assert!(!measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_rolling_window());
    assert!(!measure.is_cumulative());
    assert!(measure.is_addictive());
}

#[test]
fn measure_avg_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.average").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::Avg
    ));
    assert!(!measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_cumulative());
    assert!(!measure.is_addictive());
}

#[test]
fn measure_min_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.minimum").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::Min
    ));
    assert!(!measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_cumulative());
    assert!(measure.is_addictive());
}

#[test]
fn measure_max_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.maximum").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::Max
    ));
    assert!(!measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_cumulative());
    assert!(measure.is_addictive());
}

#[test]
fn measure_count_distinct_properties() {
    let ctx = ctx();
    let m = ctx
        .create_measure("test_measures.distinct_count")
        .unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::CountDistinct
    ));
    assert!(!measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_cumulative());
    assert!(!measure.is_addictive());
}

#[test]
fn measure_count_distinct_approx_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.approx_count").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::CountDistinctApprox
    ));
    assert!(!measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_cumulative());
    assert!(measure.is_addictive());
}

#[test]
fn measure_running_total_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.running").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::RunningTotal
    ));
    assert!(!measure.is_calculated());
    assert!(measure.is_running_total());
    assert!(!measure.is_rolling_window());
    assert!(measure.is_cumulative());
    assert!(measure.is_addictive());
}

#[test]
fn measure_number_agg_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.number_agg").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::NumberAgg
    ));
    assert!(!measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_cumulative());
    assert!(!measure.is_addictive());
}

#[test]
fn measure_calculated_number_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.calculated").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Calculated(c) if c.calc_type() == CalculatedMeasureType::Number
    ));
    assert!(measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_cumulative());
    assert!(!measure.is_addictive());
}

#[test]
fn measure_rank_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.rank_measure").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(measure.kind(), MeasureKind::Rank));
    assert!(!measure.is_calculated());
    assert!(!measure.is_running_total());
    assert!(!measure.is_cumulative());
    assert!(!measure.is_addictive());
}

#[test]
fn measure_rolling_window_properties() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.rolling_sum").unwrap();
    let measure = m.as_measure().unwrap();

    assert!(matches!(
        measure.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::Sum
    ));
    assert!(measure.is_rolling_window());
    assert!(measure.is_cumulative());
    assert!(!measure.is_running_total());
}

// ─── new_patched: valid type replacements ───────────────────────────────────

#[test]
fn new_patched_sum_to_all_valid_targets() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.total").unwrap();
    let measure = m.as_measure().unwrap();

    let cases: Vec<(&str, AggregationType)> = vec![
        ("avg", AggregationType::Avg),
        ("min", AggregationType::Min),
        ("max", AggregationType::Max),
        ("sum", AggregationType::Sum),
        ("count_distinct", AggregationType::CountDistinct),
        ("count_distinct_approx", AggregationType::CountDistinctApprox),
    ];
    for (new_type, expected_agg) in cases {
        let patched = measure
            .new_patched(Some(new_type.to_string()), vec![])
            .unwrap_or_else(|e| panic!("sum -> {} should succeed: {}", new_type, e));
        assert!(
            matches!(patched.kind(), MeasureKind::Aggregated(a) if a.agg_type() == expected_agg),
            "sum -> {}: wrong kind",
            new_type
        );
        assert_eq!(patched.full_name(), "test_measures.total");
    }
}

#[test]
fn new_patched_avg_to_sum() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.average").unwrap();
    let patched = m
        .as_measure()
        .unwrap()
        .new_patched(Some("sum".to_string()), vec![])
        .unwrap();
    assert!(matches!(
        patched.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::Sum
    ));
}

#[test]
fn new_patched_count_distinct_family() {
    let ctx = ctx();

    let cd = ctx
        .create_measure("test_measures.distinct_count")
        .unwrap();
    let patched = cd
        .as_measure()
        .unwrap()
        .new_patched(Some("count_distinct_approx".to_string()), vec![])
        .unwrap();
    assert!(matches!(
        patched.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::CountDistinctApprox
    ));

    let cda = ctx.create_measure("test_measures.approx_count").unwrap();
    let patched = cda
        .as_measure()
        .unwrap()
        .new_patched(Some("count_distinct".to_string()), vec![])
        .unwrap();
    assert!(matches!(
        patched.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::CountDistinct
    ));
}

// ─── new_patched: invalid type replacements ─────────────────────────────────

#[test]
fn new_patched_sum_invalid_targets() {
    let ctx = ctx();
    let m = ctx.create_measure("test_measures.total").unwrap();
    let measure = m.as_measure().unwrap();

    for invalid in ["number", "count", "runningTotal", "rank", "numberAgg"] {
        assert!(
            measure
                .new_patched(Some(invalid.to_string()), vec![])
                .is_err(),
            "sum -> {} should fail",
            invalid
        );
    }
}

#[test]
fn new_patched_count_distinct_to_sum_error() {
    let ctx = ctx();
    let m = ctx
        .create_measure("test_measures.distinct_count")
        .unwrap();
    assert!(m
        .as_measure()
        .unwrap()
        .new_patched(Some("sum".to_string()), vec![])
        .is_err());
}

#[test]
fn new_patched_non_patchable_types() {
    let ctx = ctx();

    let non_patchable = [
        "test_measures.cnt",
        "test_measures.calculated",
        "test_measures.rank_measure",
        "test_measures.running",
    ];
    for path in non_patchable {
        let m = ctx.create_measure(path).unwrap();
        assert!(
            m.as_measure()
                .unwrap()
                .new_patched(Some("sum".to_string()), vec![])
                .is_err(),
            "{} -> sum should fail",
            path
        );
    }
}

// ─── new_patched: no type change (None) ─────────────────────────────────────

#[test]
fn new_patched_none_preserves_kind() {
    let ctx = ctx();

    let m = ctx.create_measure("test_measures.total").unwrap();
    let patched = m.as_measure().unwrap().new_patched(None, vec![]).unwrap();
    assert!(matches!(
        patched.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::Sum
    ));

    let m = ctx.create_measure("test_measures.cnt").unwrap();
    let patched = m.as_measure().unwrap().new_patched(None, vec![]).unwrap();
    assert!(matches!(patched.kind(), MeasureKind::Count(_)));

    let m = ctx.create_measure("test_measures.calculated").unwrap();
    let patched = m.as_measure().unwrap().new_patched(None, vec![]).unwrap();
    assert!(matches!(
        patched.kind(),
        MeasureKind::Calculated(c) if c.calc_type() == CalculatedMeasureType::Number
    ));

    let m = ctx.create_measure("test_measures.rank_measure").unwrap();
    let patched = m.as_measure().unwrap().new_patched(None, vec![]).unwrap();
    assert!(matches!(patched.kind(), MeasureKind::Rank));
}

// ─── new_patched: filter addition validation ────────────────────────────────

#[test]
fn new_patched_filters_accepted_for_aggregatable_types() {
    let ctx = ctx();
    let filters = get_filter_calls(&ctx);

    let accept_filters = [
        "test_measures.total",
        "test_measures.average",
        "test_measures.minimum",
        "test_measures.maximum",
        "test_measures.cnt",
    ];
    for path in accept_filters {
        let m = ctx.create_measure(path).unwrap();
        let patched = m
            .as_measure()
            .unwrap()
            .new_patched(None, filters.clone())
            .unwrap_or_else(|e| panic!("{} + filters should succeed: {}", path, e));
        assert!(
            !patched.measure_filters().is_empty(),
            "{}: filters should be added",
            path
        );
    }
}

// BUG: countDistinct/countDistinctApprox with None type change fails filter validation
// because measure_type_str() returns camelCase ("countDistinct") but the filter
// validation match checks snake_case ("count_distinct"). Will be fixed in US-006a.
#[test]
fn new_patched_count_distinct_filters_bug() {
    let ctx = ctx();
    let filters = get_filter_calls(&ctx);

    for path in [
        "test_measures.distinct_count",
        "test_measures.approx_count",
    ] {
        let m = ctx.create_measure(path).unwrap();
        assert!(
            m.as_measure()
                .unwrap()
                .new_patched(None, filters.clone())
                .is_err(),
            "BUG: {} + filters should be Ok but fails due to camelCase/snake_case mismatch",
            path
        );
    }
}

#[test]
fn new_patched_filters_rejected_for_non_aggregatable_types() {
    let ctx = ctx();
    let filters = get_filter_calls(&ctx);

    let reject_filters = [
        "test_measures.calculated",
        "test_measures.running",
        "test_measures.rank_measure",
        "test_measures.number_agg",
    ];
    for path in reject_filters {
        let m = ctx.create_measure(path).unwrap();
        assert!(
            m.as_measure()
                .unwrap()
                .new_patched(None, filters.clone())
                .is_err(),
            "{} + filters should fail",
            path
        );
    }
}

// ─── new_patched: combined type change + filters ────────────────────────────

#[test]
fn new_patched_type_change_with_filters() {
    let ctx = ctx();
    let filters = get_filter_calls(&ctx);

    let m = ctx.create_measure("test_measures.total").unwrap();
    let patched = m
        .as_measure()
        .unwrap()
        .new_patched(Some("count_distinct".to_string()), filters)
        .unwrap();
    assert!(matches!(
        patched.kind(),
        MeasureKind::Aggregated(a) if a.agg_type() == AggregationType::CountDistinct
    ));
    assert!(!patched.measure_filters().is_empty());
}

#[test]
fn new_patched_appends_to_existing_filters() {
    let ctx = ctx();
    let m = ctx
        .create_measure("test_measures.filtered_total")
        .unwrap();
    let measure = m.as_measure().unwrap();
    let original_count = measure.measure_filters().len();
    assert!(original_count > 0);

    let new_filters = get_filter_calls(&ctx);
    let patched = measure.new_patched(None, new_filters.clone()).unwrap();
    assert_eq!(
        patched.measure_filters().len(),
        original_count + new_filters.len()
    );
}
