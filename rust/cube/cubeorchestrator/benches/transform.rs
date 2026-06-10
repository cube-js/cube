use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use cubeorchestrator::query_message_parser::QueryResult;
use cubeorchestrator::query_result_transform::TransformedData;
use cubeorchestrator::transport::{
    ConfigItem, MemberOrMemberExpression, NormalizedQuery, QueryType, ResultType,
    TransformDataRequest,
};

#[path = "common/mod.rs"]
mod common;
use common::{
    build_dataset, make_member_aliases, split_dim_measure, TimeColumn, COLUMN_COUNTS, ROW_COUNTS,
};

/// Total columns and row count used by `bench_transform_time_scenarios`.
/// Held fixed so the cells/sec figures are directly comparable to the
/// 16-col / 100k-row entries from `bench_transform`.
const SCENARIO_COL_COUNT: usize = 16;
const SCENARIO_ROW_COUNT: usize = 100_000;

fn config_item(member_type: &str) -> ConfigItem {
    ConfigItem {
        title: None,
        short_title: None,
        description: None,
        member_type: Some(member_type.to_string()),
        format: None,
        currency: None,
        meta: None,
        drill_members: None,
        drill_members_grouped: None,
        granularities: None,
        granularity: None,
    }
}

#[derive(Clone, Copy)]
enum TimeScenario {
    NoTimeDim,
    OneTimeDim,
    CustomGranularityTimeDimension,
    TwoTimeDims,
}

impl TimeScenario {
    fn label(self) -> &'static str {
        match self {
            TimeScenario::NoTimeDim => "no_time_dim",
            TimeScenario::OneTimeDim => "one_time_dim_day",
            TimeScenario::CustomGranularityTimeDimension => "one_time_dim_custom_granularity",
            TimeScenario::TwoTimeDims => "two_time_dims",
        }
    }

    fn time_columns(self) -> Vec<TimeColumn> {
        match self {
            TimeScenario::NoTimeDim => vec![],
            TimeScenario::OneTimeDim => vec![TimeColumn {
                member: "Cube.orderDate.day".to_string(),
                alias: "cube__order_date_day".to_string(),
            }],
            TimeScenario::CustomGranularityTimeDimension => vec![TimeColumn {
                member: "Cube.orderDate.fiscalQuarter".to_string(),
                alias: "cube__order_date_fiscal_quarter".to_string(),
            }],
            TimeScenario::TwoTimeDims => vec![
                TimeColumn {
                    member: "Cube.orderDate.day".to_string(),
                    alias: "cube__order_date_day".to_string(),
                },
                TimeColumn {
                    member: "Cube.shipDate.month".to_string(),
                    alias: "cube__ship_date_month".to_string(),
                },
            ],
        }
    }
}

fn build_request(
    res_type: Option<ResultType>,
    dimensions: &[(String, String)],
    measures: &[(String, String)],
    time_dims: &[TimeColumn],
) -> TransformDataRequest {
    let mut alias_to_member_name_map = HashMap::new();
    let mut annotation = HashMap::new();

    for (member, alias) in dimensions {
        alias_to_member_name_map.insert(alias.clone(), member.clone());
        annotation.insert(member.clone(), config_item("string"));
    }
    for (member, alias) in measures {
        alias_to_member_name_map.insert(alias.clone(), member.clone());
        annotation.insert(member.clone(), config_item("number"));
    }
    for td in time_dims {
        alias_to_member_name_map.insert(td.alias.clone(), td.member.clone());
        annotation.insert(td.member.clone(), config_item("time"));
    }

    let dimensions_query = dimensions
        .iter()
        .map(|(m, _)| MemberOrMemberExpression::Member(m.clone()))
        .chain(
            time_dims
                .iter()
                .map(|td| MemberOrMemberExpression::Member(td.member.clone())),
        )
        .collect();
    let measures_query = measures
        .iter()
        .map(|(m, _)| MemberOrMemberExpression::Member(m.clone()))
        .collect();

    let query = NormalizedQuery {
        measures: Some(measures_query),
        dimensions: Some(dimensions_query),
        time_dimensions: None,
        segments: None,
        limit: None,
        offset: None,
        total: None,
        total_query: None,
        timezone: Some("UTC".to_string()),
        ungrouped: None,
        response_format: None,
        filters: None,
        row_limit: None,
        order: None,
        query_type: Some(QueryType::RegularQuery),
    };

    TransformDataRequest {
        alias_to_member_name_map,
        annotation,
        query,
        query_type: Some(QueryType::RegularQuery),
        res_type,
    }
}

fn bench_transform(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransformedData::transform");

    for &col_count in COLUMN_COUNTS {
        let (dim_count, measure_count) = split_dim_measure(col_count);
        let dimensions = make_member_aliases("dim", dim_count);
        let measures = make_member_aliases("measure", measure_count);

        for &row_count in ROW_COUNTS {
            let raw = QueryResult::from_js_raw_data(build_dataset(
                row_count,
                &dimensions,
                &measures,
                &[],
            ))
            .expect("from_js_raw_data");

            // Throughput in cells/sec so numbers are comparable across widths.
            group.throughput(Throughput::Elements((row_count * col_count) as u64));

            for (label, res_type) in [
                ("compact", Some(ResultType::Compact)),
                ("columnar", Some(ResultType::Columnar)),
                ("vanilla", None),
            ] {
                let request = build_request(res_type, &dimensions, &measures, &[]);
                let id_param = format!("c{:02}_r{}", col_count, row_count);
                group.bench_with_input(BenchmarkId::new(label, id_param), &(), |b, _| {
                    b.iter(|| {
                        let result =
                            TransformedData::transform(black_box(&request), black_box(&raw))
                                .expect("transform");
                        black_box(result);
                    });
                });
            }
        }
    }

    group.finish();
}

fn bench_transform_time_scenarios(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransformedData::transform/scenarios");

    let scenarios = [
        TimeScenario::NoTimeDim,
        TimeScenario::OneTimeDim,
        TimeScenario::CustomGranularityTimeDimension,
        TimeScenario::TwoTimeDims,
    ];

    for scenario in scenarios {
        let time_dims = scenario.time_columns();
        let regular_count = SCENARIO_COL_COUNT - time_dims.len();
        let (dim_count, measure_count) = split_dim_measure(regular_count);
        let dimensions = make_member_aliases("dim", dim_count);
        let measures = make_member_aliases("measure", measure_count);

        let raw = QueryResult::from_js_raw_data(build_dataset(
            SCENARIO_ROW_COUNT,
            &dimensions,
            &measures,
            &time_dims,
        ))
        .expect("from_js_raw_data");

        // Throughput in cells/sec; total cells = row_count * total_cols, where
        // total_cols == SCENARIO_COL_COUNT regardless of scenario.
        group.throughput(Throughput::Elements(
            (SCENARIO_ROW_COUNT * SCENARIO_COL_COUNT) as u64,
        ));

        for (label, res_type) in [
            ("compact", Some(ResultType::Compact)),
            ("columnar", Some(ResultType::Columnar)),
            ("vanilla", None),
        ] {
            let request = build_request(res_type, &dimensions, &measures, &time_dims);
            let id_param = format!(
                "{}/c{:02}_r{}",
                scenario.label(),
                SCENARIO_COL_COUNT,
                SCENARIO_ROW_COUNT
            );
            group.bench_with_input(BenchmarkId::new(label, id_param), &(), |b, _| {
                b.iter(|| {
                    let result = TransformedData::transform(black_box(&request), black_box(&raw))
                        .expect("transform");
                    black_box(result);
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_transform, bench_transform_time_scenarios);
criterion_main!(benches);
