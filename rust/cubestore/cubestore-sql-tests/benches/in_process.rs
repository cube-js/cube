use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use cubestore::cluster::Cluster;
use cubestore::config::{env_parse, Config, CubeServices};
use cubestore::metastore::job::JobType;
use cubestore::metastore::{IdRow, MetaStore, MetaStoreTable, RowKey, TableId};
use cubestore::table::TableValue;
use cubestore_sql_tests::{cubestore_benches, to_rows, SqlClient};
use flate2::read::GzDecoder;
use futures::future::join_all;
use rocksdb::{Options, DB};
use std::fs;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tar::Archive;
use tokio::runtime::Builder;
use tokio::time::timeout;

fn inline_bench(criterion: &mut Criterion) {
    let benches = cubestore_benches();
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    for bench in benches.iter() {
        let config = Config::test(bench.name()).update_config(|mut c| {
            c.partition_split_threshold = 10_000_000;
            c.max_partition_split_threshold = 10_000_000;
            c.max_cached_queries = 0;
            c.max_cached_metadata = env_parse("CUBESTORE_MAX_CACHED_METADATA", 0);
            c
        });
        let _ = DB::destroy(&Options::default(), config.meta_store_path());
        let _ = fs::remove_dir_all(config.local_dir().clone());

        let (services, state) = runtime.block_on(async {
            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();
            let state = bench.setup(&services).await;
            (services, state)
        });

        criterion.bench_function(bench.name(), |b| {
            b.to_async(&runtime).iter(|| async {
                let bench = bench.clone();
                let services = services.clone();
                let state = state.clone();
                async move {
                    bench.bench(&services, state).await;
                }
                .await;
            });
        });

        runtime.block_on(async {
            services.stop_processing_loops().await.unwrap();
        });

        let _ = DB::destroy(&Options::default(), config.meta_store_path());
        let _ = fs::remove_dir_all(config.local_dir().clone());
    }
}

criterion_group!(benches, inline_bench);
criterion_main!(benches);
