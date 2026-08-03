use criterion::{criterion_group, BenchmarkId, Criterion};
use cubestore::config::Config;
use cubestore::metastore::{BaseRocksStoreFs, Column, ColumnType, MetaStore, RocksMetaStore};
use cubestore::remotefs::LocalDirRemoteFs;
use cubestore::CubeError;
use std::env;
use std::fs;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

mod tracking_allocator;

use tracking_allocator::TrackingAllocator;

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

fn prepare_metastore(name: &str) -> Result<Arc<RocksMetaStore>, CubeError> {
    let config = Config::test(name);

    let store_path = env::current_dir()
        .unwrap()
        .join("target")
        .join("bench")
        .join(format!("test-local-{}", name));
    let remote_store_path = env::current_dir()
        .unwrap()
        .join("target")
        .join("bench")
        .join(format!("test-remote-{}", name));

    let _ = fs::remove_dir_all(store_path.clone());
    let _ = fs::remove_dir_all(remote_store_path.clone());

    let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());

    RocksMetaStore::new(
        store_path.join("metastore").as_path(),
        BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
        config.config_obj(),
    )
}

async fn populate_metastore(
    metastore: &Arc<RocksMetaStore>,
    num_schemas: usize,
    tables_per_schema: usize,
) -> Result<(), CubeError> {
    for schema_idx in 0..num_schemas {
        let schema_name = format!("schema_{}", schema_idx);
        metastore.create_schema(schema_name.clone(), false).await?;

        for table_idx in 0..tables_per_schema {
            let table_name = format!("table_{}_{}", schema_idx, table_idx);
            let global_table_id = schema_idx * tables_per_schema + table_idx;
            let columns = vec![
                Column::new("name".to_string(), ColumnType::String, 1),
                Column::new("timestamp".to_string(), ColumnType::Timestamp, 2),
                Column::new("float_measure".to_string(), ColumnType::Float, 3),
                Column::new("int_measure".to_string(), ColumnType::Int, 4),
            ];

            let table_id = metastore
                .create_table(
                    schema_name.clone(),
                    table_name,
                    columns,
                    None,   // locations
                    None,   // import_format
                    vec![], // indexes
                    false,  // is_ready
                    None,   // build_range_end
                    None,   // seal_at
                    None,   // select_statement
                    None,   // source_columns
                    None,   // stream_offset
                    None,   // unique_key_column_names
                    None,   // aggregates
                    None,   // partition_split_threshold
                    None,   // trace_obj
                    false,  // drop_if_exists
                    None,   // extension
                )
                .await?;

            // Make some tables ready and some not ready for realistic testing
            if global_table_id % 4 != 3 {
                metastore.table_ready(table_id.get_id(), true).await?;
            }
        }
    }

    Ok(())
}

async fn bench_get_tables_with_path(
    metastore: &Arc<RocksMetaStore>,
    include_non_ready: bool,
    iterations: usize,
) {
    for _ in 0..iterations {
        let result = metastore.get_tables_with_path(include_non_ready).await;
        assert!(result.is_ok());
    }
}

fn do_get_tables_with_path_bench(
    c: &mut Criterion,
    runtime: &Runtime,
    num_schemas: usize,
    tables_per_schema: usize,
    iterations: usize,
) {
    let total_tables = num_schemas * tables_per_schema;
    let metastore = runtime.block_on(async {
        let metastore =
            prepare_metastore(&format!("get_tables_with_path_{}", total_tables)).unwrap();
        populate_metastore(&metastore, num_schemas, tables_per_schema)
            .await
            .unwrap();
        metastore
    });

    c.bench_with_input(
        BenchmarkId::new("get_tables_with_path_include_non_ready_true", total_tables),
        &iterations,
        |b, &iterations| {
            b.to_async(runtime)
                .iter(|| bench_get_tables_with_path(&metastore, true, iterations));
        },
    );

    c.bench_with_input(
        BenchmarkId::new("get_tables_with_path_include_non_ready_false", total_tables),
        &iterations,
        |b, &iterations| {
            b.to_async(runtime)
                .iter(|| bench_get_tables_with_path(&metastore, false, iterations));
        },
    );
}

fn do_cold_vs_warm_cache_bench(
    c: &mut Criterion,
    runtime: &Runtime,
    num_schemas: usize,
    tables_per_schema: usize,
) {
    let cold_metastore = runtime.block_on(async {
        let metastore = prepare_metastore("warm_cache").unwrap();
        populate_metastore(&metastore, num_schemas, tables_per_schema)
            .await
            .unwrap();
        metastore
    });

    let warm_metastore = runtime.block_on(async {
        let metastore = prepare_metastore("cold_cache").unwrap();
        populate_metastore(&metastore, num_schemas, tables_per_schema)
            .await
            .unwrap();
        metastore
    });

    c.bench_function("get_tables_with_path_cold_cache", |b| {
        b.to_async(runtime).iter_batched(
            || cold_metastore.reset_cached_tables(),
            async |_| {
                let result = cold_metastore.get_tables_with_path(false).await;
                assert!(result.is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("get_tables_with_path_warm_cache", |b| {
        b.to_async(runtime).iter(async || {
            let result = warm_metastore.get_tables_with_path(false).await;
            assert!(result.is_ok());
        });
    });
}

fn do_benches(c: &mut Criterion) {
    ALLOCATOR.reset_stats();
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    do_get_tables_with_path_bench(c, &runtime, 10, 10, 100);
    do_get_tables_with_path_bench(c, &runtime, 50, 20, 50);
    do_get_tables_with_path_bench(c, &runtime, 25, 1000, 10);

    do_cold_vs_warm_cache_bench(c, &runtime, 20, 50);
}

criterion_group!(benches, do_benches);

fn main() {
    benches();
    ALLOCATOR.print_stats();
}
