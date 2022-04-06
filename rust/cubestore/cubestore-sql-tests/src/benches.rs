use std::any::Any;
use crate::to_rows;
use async_trait::async_trait;
use cubestore::cluster::Cluster;
use cubestore::config::{Config, CubeServices, env_parse};
use cubestore::metastore::job::JobType;
use cubestore::metastore::{MetaStoreTable, RowKey, TableId};
use cubestore::table::TableValue;
use cubestore::util::strings::path_to_string;
use cubestore::CubeError;
use flate2::read::GzDecoder;
use futures::future::join_all;
use std::io::Cursor;
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
    ];
}

pub struct SimpleBenchState {
    query: String
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
        Ok(Arc::new(SimpleBenchState { query: "SELECT 23".to_string() }))
    }

    async fn bench(
        self: &Self,
        services: &CubeServices,
        state: Arc<BenchState>,
    ) -> Result<(), CubeError> {
        let state = state.downcast_ref::<SimpleBenchState>().ok_or(CubeError::internal("bad state".to_string()))?;
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
// CUBESTORE_MAX_CACHED_METADATA=0 cargo bench parquet_metadata_cache
// CUBESTORE_MAX_CACHED_METADATA=100 cargo bench parquet_metadata_cache
pub struct ParquetMetadataCacheBench;
#[async_trait]
impl Bench for ParquetMetadataCacheBench {
    fn config(self: &Self, prefix: &str) -> (String, Config) {
        let name = config_name(prefix, "parquet_metadata_cache");
        let config = Config::test(name.as_str()).update_config(|mut c| {
            c.partition_split_threshold = 10_000_000;
            c.max_partition_split_threshold = 10_000_000;
            c.max_cached_queries = 0;
            c.max_cached_metadata = env_parse("CUBESTORE_MAX_CACHED_METADATA", 0);
            c
        });
        (name, config)
    }

    async fn setup(self: &Self, services: &CubeServices) -> Result<Arc<BenchState>, CubeError> {
        let path = download_and_unzip("https://media.githubusercontent.com/media/cube-js/testing-fixtures/master/github-commits-000.tar.gz", "github-commits-000.csv").await?;

        let _ = services
            .sql_service
            .exec_query("CREATE SCHEMA IF NOT EXISTS test")
            .await?;

        let _ = services.sql_service
            .exec_query(format!("CREATE TABLE test.table (`repo` text, `email` text, `commit_count` int) WITH (input_format = 'csv') LOCATION '{}'", path).as_str())
            .await?;

        compact_partitions(&services).await?;

        let state = Arc::new(());

        // warmup metadata cache
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

async fn download_and_unzip(url: &str, filename: &str) -> Result<String, CubeError> {
    let dir = std::env::current_dir()?.join("data");
    let path = dir.join(filename);
    if !path.exists() {
        println!("Downloading {}", filename);
        let response = reqwest::get(url).await?;
        let content = Cursor::new(response.bytes().await?);
        let tarfile = GzDecoder::new(content);
        let mut archive = Archive::new(tarfile);
        archive.unpack(dir)?;
    }
    path_to_string(path)
}

async fn compact_partitions(services: &CubeServices) -> Result<(), CubeError> {
    let partitions = services.meta_store.partition_table().all_rows().await?;
    let scheduler = services.scheduler.clone();
    join_all(
        partitions
            .iter()
            .map(|p| scheduler.schedule_partition_to_compact(&p)),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;
    let jobs = partitions
        .iter()
        .map(|p| {
            (
                RowKey::Table(TableId::Partitions, p.get_id()),
                JobType::PartitionCompaction,
            )
        })
        .collect::<Vec<_>>();
    let listener = services.cluster.job_result_listener();
    timeout(Duration::from_secs(10), listener.wait_for_job_results(jobs)).await??;
    Ok(())
}
