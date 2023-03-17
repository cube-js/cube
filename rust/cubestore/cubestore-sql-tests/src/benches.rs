use crate::to_rows;
use async_trait::async_trait;
use cubestore::cluster::Cluster;
use cubestore::config::{env_parse, Config, CubeServices};
use cubestore::table::TableValue;
use cubestore::util::strings::path_to_string;
use cubestore::CubeError;
use flate2::read::GzDecoder;
use std::any::Any;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tar::Archive;
use tokio::time::timeout;

pub type BenchState = dyn Any + Send + Sync;

#[async_trait]
pub trait Bench: Send + Sync {
    fn config(self: &Self, prefix: &str) -> (String, Config);
    async fn setup(self: &Self, services: &CubeServices) -> Result<Arc<BenchState>, CubeError>;
    async fn bench(
        self: &Self,
        services: &CubeServices,
        state: Arc<BenchState>,
    ) -> Result<(), CubeError>;
}

fn config_name(prefix: &str, name: &str) -> String {
    format!("{}::{}", prefix, name)
}

pub fn cubestore_benches() -> Vec<Arc<dyn Bench>> {
    return vec![
        Arc::new(SimpleBench {}),
        Arc::new(ParquetMetadataCacheBench {}),
        Arc::new(CacheSetGetBench {}),
    ];
}

pub struct SimpleBenchState {
    query: String,
}
pub struct SimpleBench;
#[async_trait]
impl Bench for SimpleBench {
    fn config(self: &Self, prefix: &str) -> (String, Config) {
        let name = config_name(prefix, "simple");
        let config = Config::test(name.as_str());
        (name, config)
    }

    async fn setup(self: &Self, _services: &CubeServices) -> Result<Arc<BenchState>, CubeError> {
        Ok(Arc::new(SimpleBenchState {
            query: "SELECT 23".to_string(),
        }))
    }

    async fn bench(
        self: &Self,
        services: &CubeServices,
        state: Arc<BenchState>,
    ) -> Result<(), CubeError> {
        let state = state
            .downcast_ref::<SimpleBenchState>()
            .ok_or(CubeError::internal("bad state".to_string()))?;
        let r = services
            .sql_service
            .exec_query(state.query.as_str())
            .await?;
        let rows = to_rows(&r);
        assert_eq!(rows, vec![vec![TableValue::Int(23)]]);
        Ok(())
    }
}

// To compare, bench without / with bench enabled.
// CUBESTORE_METADATA_CACHE_MAX_CAPACITY_BYTES=0 cargo bench parquet_metadata_cache
// CUBESTORE_METADATA_CACHE_MAX_CAPACITY_BYTES=1000000 cargo bench parquet_metadata_cache
pub struct ParquetMetadataCacheBench;
#[async_trait]
impl Bench for ParquetMetadataCacheBench {
    fn config(self: &Self, prefix: &str) -> (String, Config) {
        let name = config_name(prefix, "parquet_metadata_cache");
        let config = Config::test(name.as_str()).update_config(|mut c| {
            c.partition_split_threshold = 10_000_000;
            c.max_partition_split_threshold = 10_000_000;
            c.max_cached_queries = 0;
            c.metadata_cache_max_capacity_bytes =
                env_parse("CUBESTORE_METADATA_CACHE_MAX_CAPACITY_BYTES", 0);
            c.metadata_cache_time_to_idle_secs = 1000;
            c
        });
        (name, config)
    }

    async fn setup(self: &Self, services: &CubeServices) -> Result<Arc<BenchState>, CubeError> {
        let dataset_path = download_and_unzip(
            "https://github.com/cube-js/testing-fixtures/raw/master/github-commits.tar.gz",
            "github-commits",
        )
        .await?;
        let path = dataset_path.join("github-commits-000.csv");

        let _ = services
            .sql_service
            .exec_query("CREATE SCHEMA IF NOT EXISTS test")
            .await?;

        let _ = services.sql_service
            .exec_query(format!("CREATE TABLE test.table (`repo` text, `email` text, `commit_count` int) WITH (input_format = 'csv') LOCATION '{}'", path_to_string(path)?).as_str())
            .await?;

        // Wait for all pending (compaction) jobs to finish.
        wait_for_all_jobs(&services).await?;

        let state = Arc::new(());

        // Warmup metadata cache.
        self.bench(services, state.clone()).await?;

        Ok(state)
    }

    async fn bench(
        self: &Self,
        services: &CubeServices,
        _state: Arc<BenchState>,
    ) -> Result<(), CubeError> {
        let repo = "2degrees/twod.wsgi";
        let r = services
            .sql_service
            .exec_query(
                format!(
                    "SELECT COUNT(*) FROM test.table WHERE repo = '{}' GROUP BY repo",
                    repo
                )
                .as_str(),
            )
            .await?;
        let rows = to_rows(&r);
        assert_eq!(rows, vec![vec![TableValue::Int(6)]]);
        Ok(())
    }
}

pub struct CacheSetGetBench;
#[async_trait]
impl Bench for CacheSetGetBench {
    fn config(self: &Self, prefix: &str) -> (String, Config) {
        let name = config_name(prefix, "cache_set_get");
        let config = Config::test(name.as_str()).update_config(|c| c);
        (name, config)
    }

    async fn setup(self: &Self, services: &CubeServices) -> Result<Arc<BenchState>, CubeError> {
        services
            .sql_service
            .exec_query("CACHE SET TTL 600 'my_key' 'my_value'")
            .await?;

        let state = Arc::new(());
        Ok(state)
    }

    async fn bench(
        self: &Self,
        services: &CubeServices,
        _state: Arc<BenchState>,
    ) -> Result<(), CubeError> {
        let r = services
            .sql_service
            .exec_query("CACHE GET 'my_key'")
            .await?;

        let rows = to_rows(&r);
        assert_eq!(rows, vec![vec![TableValue::String("my_value".to_string())]]);

        Ok(())
    }
}

async fn download_and_unzip(url: &str, dataset: &str) -> Result<Box<Path>, CubeError> {
    let root = std::env::current_dir()?.join("data");
    let dataset_path = root.join(dataset);
    if !dataset_path.exists() {
        println!("Downloading {}", dataset);
        let response = reqwest::get(url).await?;
        let content = Cursor::new(response.bytes().await?);
        let tarfile = GzDecoder::new(content);
        let mut archive = Archive::new(tarfile);
        archive.unpack(root)?;
    }
    assert!(dataset_path.exists());
    Ok(dataset_path.into_boxed_path())
}

async fn wait_for_all_jobs(services: &CubeServices) -> Result<(), CubeError> {
    let wait_for = services
        .meta_store
        .all_jobs()
        .await?
        .iter()
        .map(|j| {
            (
                j.get_row().row_reference().clone(),
                j.get_row().job_type().clone(),
            )
        })
        .collect();
    let listener = services.cluster.job_result_listener();
    timeout(
        Duration::from_secs(10),
        listener.wait_for_job_results(wait_for),
    )
    .await??;
    Ok(())
}
