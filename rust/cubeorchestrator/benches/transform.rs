use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use cubeorchestrator::query_message_parser::QueryResult;
use cubeorchestrator::query_result_transform::{DBResponsePrimitive, TransformedData};
use cubeorchestrator::transport::{
    ConfigItem, JsRawData, MemberOrMemberExpression, NormalizedQuery, QueryType, ResultType,
    TransformDataRequest,
};
use indexmap::IndexMap;

const DIMENSIONS: &[(&str, &str)] = &[
    ("Sales.country", "sales__country"),
    ("Sales.city", "sales__city"),
    ("Sales.region", "sales__region"),
    ("Sales.product", "sales__product"),
    ("Sales.category", "sales__category"),
    ("Sales.segment", "sales__segment"),
];

const MEASURES: &[(&str, &str)] = &[
    ("Sales.revenue", "sales__revenue"),
    ("Sales.profit", "sales__profit"),
    ("Sales.discount", "sales__discount"),
    ("Sales.count", "sales__count"),
];

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

fn build_request(res_type: Option<ResultType>) -> TransformDataRequest {
    let mut alias_to_member_name_map = HashMap::new();
    let mut annotation = HashMap::new();

    for (member, alias) in DIMENSIONS {
        alias_to_member_name_map.insert((*alias).to_string(), (*member).to_string());
        annotation.insert((*member).to_string(), config_item("string"));
    }
    for (member, alias) in MEASURES {
        alias_to_member_name_map.insert((*alias).to_string(), (*member).to_string());
        annotation.insert((*member).to_string(), config_item("number"));
    }

    let dimensions = DIMENSIONS
        .iter()
        .map(|(m, _)| MemberOrMemberExpression::Member((*m).to_string()))
        .collect();
    let measures = MEASURES
        .iter()
        .map(|(m, _)| MemberOrMemberExpression::Member((*m).to_string()))
        .collect();

    let query = NormalizedQuery {
        measures: Some(measures),
        dimensions: Some(dimensions),
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

fn build_dataset(row_count: usize) -> JsRawData {
    let dim_count = DIMENSIONS.len();
    let total_cols = dim_count + MEASURES.len();
    let mut rows = Vec::with_capacity(row_count);

    for i in 0..row_count {
        let mut row = IndexMap::with_capacity(total_cols);
        for (j, (_, alias)) in DIMENSIONS.iter().enumerate() {
            row.insert(
                (*alias).to_string(),
                DBResponsePrimitive::String(format!("dim_{}_{}", j, i % 1000)),
            );
        }
        for (j, (_, alias)) in MEASURES.iter().enumerate() {
            row.insert(
                (*alias).to_string(),
                DBResponsePrimitive::Number(((i * (j + 1)) as f64) * 0.5),
            );
        }
        rows.push(row);
    }

    rows
}

fn bench_transform(c: &mut Criterion) {
    let mut group = c.benchmark_group("TransformedData::transform");

    for &row_count in &[1_000usize, 10_000, 50_000, 100_000] {
        let raw =
            QueryResult::from_js_raw_data(build_dataset(row_count)).expect("from_js_raw_data");

        group.throughput(Throughput::Elements(row_count as u64));

        for (label, res_type) in [
            ("compact", Some(ResultType::Compact)),
            ("columnar", Some(ResultType::Columnar)),
            ("vanilla", None),
        ] {
            let request = build_request(res_type);
            group.bench_with_input(BenchmarkId::new(label, row_count), &row_count, |b, _| {
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

criterion_group!(benches, bench_transform);
criterion_main!(benches);
