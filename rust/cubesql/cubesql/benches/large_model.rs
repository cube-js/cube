use std::{env::set_var, sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};

use cubeclient::models::{V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaType};
use cubesql::{
    compile::test::{
        get_test_tenant_ctx_with_meta,
        rewrite_engine::{
            create_test_postgresql_cube_context, query_to_logical_plan, rewrite_rules,
            rewrite_runner,
        },
    },
    transport::{CubeMetaDimension, MetaContext},
};
use egg::StopReason;
use itertools::Itertools;

macro_rules! bench_large_model {
    ($DIMS:expr, $NAME:expr, $QUERY_FN:expr, $CRITERION:expr) => {{
        let context = Arc::new(
            futures::executor::block_on(create_test_postgresql_cube_context(
                get_large_model_test_tenant_ctx($DIMS),
            ))
            .unwrap(),
        );

        let plan = query_to_logical_plan($QUERY_FN($DIMS), &context);
        let rules = rewrite_rules(context.clone());

        let bench_name = format!("large_model_{}_{}", $DIMS, $NAME);
        $CRITERION.bench_function(&bench_name, |b| {
            b.iter(|| {
                let context = context.clone();
                let plan = plan.clone();
                let rules = rules.clone();

                let runner = rewrite_runner(plan, context);
                let stop_reason = runner.run(&rules).stop_reason.unwrap();
                if !matches!(stop_reason, StopReason::Saturated) {
                    panic!(
                        "Error running {} benchmark: stop reason is {:?}",
                        bench_name, stop_reason
                    );
                }
            })
        });
    }};
}

pub fn get_large_model_test_tenant_ctx(dims: usize) -> Arc<MetaContext> {
    get_test_tenant_ctx_with_meta(get_large_model_test_meta(dims))
}

pub fn get_large_model_test_meta(dims: usize) -> Vec<V1CubeMeta> {
    if dims < 1 {
        panic!("Number of dimensions should be at least 1");
    }

    let cube_name = format!("LargeCube_{}", dims);
    vec![V1CubeMeta {
        name: cube_name.clone(),
        description: None,
        title: None,
        r#type: V1CubeMetaType::Cube,
        measures: vec![
            V1CubeMetaMeasure {
                name: format!("{}.count", cube_name),
                title: None,
                short_title: None,
                description: None,
                r#type: "number".to_string(),
                agg_type: Some("count".to_string()),
                meta: None,
                alias_member: None,
            },
            V1CubeMetaMeasure {
                name: format!("{}.sum", cube_name),
                title: None,
                short_title: None,
                description: None,
                r#type: "number".to_string(),
                agg_type: Some("sum".to_string()),
                meta: None,
                alias_member: None,
            },
        ],
        dimensions: (1..=dims)
            .map(|n| V1CubeMetaDimension {
                name: format!("{}.n{}", cube_name, n),
                r#type: "number".to_string(),
                ..CubeMetaDimension::default()
            })
            .collect(),
        segments: vec![],
        joins: None,
        folders: None,
        nested_folders: None,
        hierarchies: None,
        meta: None,
    }]
}

fn select_one_dimension(dims: usize) -> String {
    format!(
        r#"
        SELECT n1 AS n1
        FROM LargeCube_{}
        GROUP BY 1
        "#,
        dims,
    )
}

fn select_wildcard(dims: usize) -> String {
    format!(
        r#"
        SELECT *
        FROM LargeCube_{}
        "#,
        dims,
    )
}

fn select_all_dimensions(dims: usize) -> String {
    let select_expr = Itertools::intersperse(
        (1..=dims).map(|n| format!("n{} AS n{}", n, n)),
        ", ".to_string(),
    )
    .collect::<String>();
    let group_expr = Itertools::intersperse((1..=dims).map(|n| n.to_string()), ", ".to_string())
        .collect::<String>();
    format!(
        r#"
        SELECT {}
        FROM LargeCube_{}
        GROUP BY {}
        "#,
        select_expr, dims, group_expr,
    )
}

fn select_all_dimensions_with_filter(dims: usize) -> String {
    let select_expr = Itertools::intersperse(
        (1..=dims).map(|n| format!("n{} AS n{}", n, n)),
        ", ".to_string(),
    )
    .collect::<String>();
    let group_expr = Itertools::intersperse((1..=dims).map(|n| n.to_string()), ", ".to_string())
        .collect::<String>();
    format!(
        r#"
        SELECT {}
        FROM LargeCube_{}
        WHERE n1 > 10
        GROUP BY {}
        "#,
        select_expr, dims, group_expr,
    )
}

fn select_many_filters(dims: usize) -> String {
    let select_expr = Itertools::intersperse(
        (1..=dims).map(|n| format!("n{} AS n{}", n, n)),
        ", ".to_string(),
    )
    .collect::<String>();
    let filter_expr = Itertools::intersperse(
        (1..=dims).map(|n| format!("n{} > 10", n)),
        " AND ".to_string(),
    )
    .collect::<String>();
    let group_expr = Itertools::intersperse((1..=dims).map(|n| n.to_string()), ", ".to_string())
        .collect::<String>();
    format!(
        r#"
        SELECT {}
        FROM LargeCube_{}
        WHERE {}
        GROUP BY {}
        "#,
        select_expr, dims, filter_expr, group_expr,
    )
}

fn large_model_100_dims(c: &mut Criterion) {
    // This is required for `select_many_filters` test, remove after flattening filters
    set_var("CUBESQL_REWRITE_MAX_NODES", "100000");

    let dims = 100;
    bench_large_model!(dims, "select_one_dimension", select_one_dimension, c);
    bench_large_model!(dims, "select_wildcard", select_wildcard, c);
    bench_large_model!(dims, "select_all_dimensions", select_all_dimensions, c);
    bench_large_model!(
        dims,
        "select_all_dimensions_with_filter",
        select_all_dimensions_with_filter,
        c
    );
    bench_large_model!(dims, "select_many_filters", select_many_filters, c);
}

fn large_model_300_dims(c: &mut Criterion) {
    let dims = 300;
    bench_large_model!(dims, "select_one_dimension", select_one_dimension, c);
    bench_large_model!(dims, "select_wildcard", select_wildcard, c);
    bench_large_model!(dims, "select_all_dimensions", select_all_dimensions, c);
    bench_large_model!(
        dims,
        "select_all_dimensions_with_filter",
        select_all_dimensions_with_filter,
        c
    );
    // `select_many_filters` takes too long with 300 filters; requires flattening
    //bench_large_model!(dims, "select_many_filters", select_many_filters, c);
}

fn large_model_1000_dims(c: &mut Criterion) {
    let dims = 1000;
    bench_large_model!(dims, "select_one_dimension", select_one_dimension, c);
    bench_large_model!(dims, "select_wildcard", select_wildcard, c);
    bench_large_model!(dims, "select_all_dimensions", select_all_dimensions, c);
    bench_large_model!(
        dims,
        "select_all_dimensions_with_filter",
        select_all_dimensions_with_filter,
        c
    );
    // `select_many_filters` takes too long with 1000 filters; requires flattening
    //bench_large_model!(dims, "select_many_filters", select_many_filters, c);
}

criterion_group! {
    name = large_model;
    config = Criterion::default().measurement_time(Duration::from_secs(15)).sample_size(10);
    targets = large_model_100_dims, large_model_300_dims, large_model_1000_dims
}
criterion_main!(large_model);
