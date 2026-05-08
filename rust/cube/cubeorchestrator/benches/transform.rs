use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use cubeorchestrator::query_message_parser::QueryResult;
use cubeorchestrator::query_result_transform::{DBResponsePrimitive, TransformedData};
use cubeorchestrator::transport::{
    ConfigItem, JsRawColumnarData, MemberOrMemberExpression, NormalizedQuery, QueryType,
    ResultType, TransformDataRequest,
};

const ROW_COUNTS: &[usize] = &[1_000, 10_000, 50_000, 100_000];
const COLUMN_COUNTS: &[usize] = &[8, 16, 32, 64];

/// Total columns and row count used by `bench_transform_time_scenarios`.
/// Held fixed so the cells/sec figures are directly comparable to the
/// 16-col / 100k-row entries from `bench_transform`.
const SCENARIO_COL_COUNT: usize = 16;
const SCENARIO_ROW_COUNT: usize = 100_000;

/// Split a target column count into ~60% dimensions and ~40% measures.
fn split_dim_measure(col_count: usize) -> (usize, usize) {
    let dim_count = (col_count * 6) / 10;
    let measure_count = col_count - dim_count;
    (dim_count, measure_count)
}

fn make_member_aliases(prefix: &str, count: usize) -> Vec<(String, String)> {
    (0..count)
        .map(|i| {
            (
                format!("Sales.{}{}", prefix, i),
                format!("sales__{}{}", prefix, i),
            )
        })
        .collect()
}

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

#[derive(Clone)]
struct TimeColumn {
    member: String,
    alias: String,
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
        renew_query: None,
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

fn build_dataset(
    row_count: usize,
    dimensions: &[(String, String)],
    measures: &[(String, String)],
    time_dims: &[TimeColumn],
) -> JsRawColumnarData {
    let total_cols = dimensions.len() + measures.len() + time_dims.len();
    let mut members = Vec::with_capacity(total_cols);
    let mut columns: Vec<Vec<DBResponsePrimitive>> = Vec::with_capacity(total_cols);

    for (j, (_, alias)) in dimensions.iter().enumerate() {
        members.push(alias.clone());
        let mut col = Vec::with_capacity(row_count);
        for i in 0..row_count {
            col.push(DBResponsePrimitive::String(format!(
                "dim_{}_{}",
                j,
                i % 1000
            )));
        }
        columns.push(col);
    }
    for (j, (_, alias)) in measures.iter().enumerate() {
        members.push(alias.clone());
        let mut col = Vec::with_capacity(row_count);
        for i in 0..row_count {
            col.push(DBResponsePrimitive::Number(((i * (j + 1)) as f64) * 0.5));
        }
        columns.push(col);
    }
    for (j, td) in time_dims.iter().enumerate() {
        members.push(td.alias.clone());
        let mut col = Vec::with_capacity(row_count);
        for i in 0..row_count {
            // Format mirrors typical CubeStore output: ISO-8601 with millisecond
            // fractional and no timezone.
            let month = ((i + j) % 12) + 1;
            let day = ((i / 12) % 28) + 1;
            col.push(DBResponsePrimitive::String(format!(
                "2024-{:02}-{:02}T00:00:00.000",
                month, day
            )));
        }
        columns.push(col);
    }

    JsRawColumnarData { members, columns }
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
