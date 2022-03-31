use std::fs;
use async_trait::async_trait;
use std::sync::Arc;
use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Builder;
use rocksdb::{Options, DB};
use cubestore::config::{Config, CubeServices, env_parse};
use cubestore::table::TableValue;
use cubestore_sql_tests::{SqlClient, to_rows};

#[async_trait]
pub trait Bench<T>: Send + Sync {
    fn name(self: &Self) -> &'static str;
    async fn setup(self: &Self, services: &CubeServices) -> T;
    async fn bench(self: &Self, services: &CubeServices, state: &T);
}

#[derive(Debug)]
struct ParquetMetadataCacheBenchState {
    repos: Arc<Vec<String>>,
}

struct ParquetMetadataCacheBench;
#[async_trait]
impl Bench<ParquetMetadataCacheBenchState> for ParquetMetadataCacheBench {
    fn name(self: &Self) -> &'static str {
        "parquet_metadata_cache"
    }

    async fn setup(self: &Self, services: &CubeServices) -> ParquetMetadataCacheBenchState {
        let _ = services.sql_service
            .exec_query("CREATE SCHEMA IF NOT EXISTS test")
            .await
            .unwrap();
        let path = "./github-commits-000.csv";
        let _ = services.sql_service
            .exec_query(format!("CREATE TABLE test.table (`repo` text, `email` text, `commit_count` int) WITH (input_format = 'csv') LOCATION '{}'", path).as_str())
            .await
            .unwrap();

        let r = services.sql_service.exec_query("SELECT repo FROM test.table GROUP BY repo").await.unwrap();
        let repos = to_rows(&r).iter().map(|row| {
            if let TableValue::String(repo) = &row[0] {
                repo.clone()
            } else {
                panic!("Not a string.")
            }
        }).collect::<Vec<_>>();
        assert_eq!(repos.len(), 51533);
        let state = ParquetMetadataCacheBenchState { repos: Arc::new(repos) };
        // warmup metadata cache
        self.bench(services, &state).await;
        state
    }

    async fn bench(self: &Self, services: &CubeServices, state: &ParquetMetadataCacheBenchState) {
        let repo = &state.repos[12345];
        assert_eq!(repo, "2degrees/twod.wsgi");
        let r = services.sql_service.exec_query(format!("SELECT COUNT(*) FROM test.table WHERE repo = '{}' GROUP BY repo", repo).as_str()).await.unwrap();
        let rows = to_rows(&r);
        assert_eq!(rows, vec![vec![TableValue::Int(6)]]);
    }
}

fn inline_bench(criterion: &mut Criterion) {
    let bench = Arc::new(ParquetMetadataCacheBench {});

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let config = Config::test(bench.name()).update_config(|mut c| {
        c.partition_split_threshold = 10_000_000;
        c.max_partition_split_threshold = 10_000_000;
        c.max_cached_queries = 0;
        c.max_cached_metadata = env_parse("CUBESTORE_MAX_CACHED_METADATA", 0);
        c
    });
    let _ = DB::destroy(&Options::default(), config.meta_store_path());
    let _ = fs::remove_dir_all(config.local_dir().clone());

    {
        let (services, state) = runtime.block_on(async {
            let services = config.configure().await;
            services.start_processing_loops().await.unwrap();
            let state = Arc::new(bench.setup(&services).await);
            (services, state)
        });

        criterion.bench_function(bench.name(), |b| {
            b.to_async(&runtime).iter(|| async {
                let bench = bench.clone();
                let services = services.clone();
                let state = state.clone();
                async move {
                    bench.bench(&services, &state).await;
                }.await;
            });
        });

        runtime.block_on(async {
            services.stop_processing_loops().await.unwrap();
        });
    }

    // let _ = DB::destroy(&Options::default(), config.meta_store_path());
    // let _ = fs::remove_dir_all(config.local_dir().clone());
}

criterion_group!(benches, inline_bench);
criterion_main!(benches);
