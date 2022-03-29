use std::fs;
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use std::future::Future;
use std::panic::RefUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use log::debug;
use tokio::runtime::Builder;
use rocksdb::{Options, DB};
use cubestore::cluster;
use cubestore::config::{Config, CubeServices, env_parse};
use cubestore::sql::SqlService;
use cubestore::table::TableValue;
use cubestore::util::respawn;
use cubestore_sql_tests::multiproc::{multiproc_child_main, MultiProcTest, run_multiproc_test, Runtime, SignalInit, WaitCompletion, WorkerProc};
use cubestore_sql_tests::{SqlClient, to_rows};

#[async_trait]
pub trait Bench<T>: Send + Sync {
    fn name(self: &Self) -> &'static str;
    async fn setup(self: &Self, service: Arc<dyn SqlService>) -> T;
    async fn bench(self: &Self, service: Arc<dyn SqlService>, state: &T) -> T;
}

#[derive(Debug)]
struct ParquetMetadataCacheBenchState {
    repos: Arc<Vec<String>>,
    pos: usize,
}
impl ParquetMetadataCacheBenchState {
    fn next(self: &Self) -> (Self, &str) {
        // let pos = self.pos + 1;
        let pos = self.pos;
        (ParquetMetadataCacheBenchState { repos: self.repos.clone(), pos }, self.repos[self.pos].as_str())
    }
}
struct ParquetMetadataCacheBench;
#[async_trait]
impl Bench<ParquetMetadataCacheBenchState> for ParquetMetadataCacheBench {
    fn name(self: &Self) -> &'static str {
        "parquet_metadata_cache"
    }

    async fn setup(self: &Self, service: Arc<dyn SqlService>) -> ParquetMetadataCacheBenchState {
        let _ = service
            .exec_query("CREATE SCHEMA IF NOT EXISTS test")
            .await
            .unwrap();
        let path = "./github-commits-000.csv";
        let _ = service
            .exec_query(format!("CREATE TABLE test.table (`repo` text, `email` text, `commit_count` int) WITH (input_format = 'csv') LOCATION '{}'", path).as_str())
            .await
            .unwrap();
        let r = service.exec_query("SELECT repo FROM test.table GROUP BY repo").await.unwrap();
        let repos = to_rows(&r).iter().map(|row| {
            if let TableValue::String(repo) = &row[0] {
                repo.clone()
            } else {
                panic!("Not a string.")
            }
        }).collect::<Vec<_>>();
        assert_eq!(repos.len(), 51533);
        let state = ParquetMetadataCacheBenchState { repos: Arc::new(repos), pos: 1234 };
        // warmup cache
        self.bench(service, &state).await
    }

    async fn bench(self: &Self, service: Arc<dyn SqlService>, state: &ParquetMetadataCacheBenchState) -> ParquetMetadataCacheBenchState {
        let (state, repo) = state.next();
        let r = service.exec_query(format!("SELECT COUNT(*) FROM test.table WHERE repo = '{}' GROUP BY repo", repo).as_str()).await.unwrap();
        let rows = to_rows(&r);
        assert_eq!(rows.len(), 1);
        state
    }
}

fn inline_bench(criterion: &mut Criterion) {
    let bench = Arc::new(ParquetMetadataCacheBench {});

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let config = Config::test(bench.name()).update_config(|mut c| {
        c.max_cached_metadata = env_parse("CUBESTORE_MAX_CACHED_METADATA", 0);
        c
    });
    let _ = DB::destroy(&Options::default(), config.meta_store_path());
    let _ = fs::remove_dir_all(config.local_dir().clone());

    {
        let (services, state) = runtime.block_on(async {
            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();
            let state = bench.setup(services.sql_service.clone()).await;
            (services, Arc::new(Mutex::new(state)))
        });

        criterion.bench_function(bench.name(), |b| {
            b.to_async(&runtime).iter(|| async {
                let bench = bench.clone();
                let sql_service = services.sql_service.clone();
                let state = state.clone();
                async move {
                    let mut state = state.lock().unwrap();
                    *state = bench.bench(sql_service, &state).await;
                }.await;
            });
        });

        runtime.block_on(async {
            services.stop_processing_loops().await.unwrap();
        });
    }

    let _ = DB::destroy(&Options::default(), config.meta_store_path());
    let _ = fs::remove_dir_all(config.local_dir().clone());
}

criterion_group!(benches, inline_bench);
criterion_main!(benches);
