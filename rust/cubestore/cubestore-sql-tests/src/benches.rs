use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use flate2::read::GzDecoder;
use futures::future::join_all;
use tar::Archive;
use tokio::time::timeout;
use cubestore::cluster::Cluster;
use cubestore::config::CubeServices;
use cubestore::metastore::{MetaStoreTable, RowKey, TableId};
use cubestore::metastore::job::JobType;
use cubestore::table::TableValue;
use crate::to_rows;

pub trait BenchState: Send + Sync {}

#[async_trait]
pub trait Bench: Send + Sync {
    fn name(self: &Self) -> &'static str;
    async fn setup(self: &Self, services: &CubeServices) -> Arc<dyn BenchState>;
    async fn bench(self: &Self, services: &CubeServices, state: Arc<dyn BenchState>);
}

#[derive(Debug)]
pub struct EmptyBenchState {}
impl BenchState for EmptyBenchState {}

pub struct ParquetMetadataCacheBench;
#[async_trait]
impl Bench for ParquetMetadataCacheBench {
    fn name(self: &Self) -> &'static str {
        "parquet_metadata_cache"
    }

    async fn setup(self: &Self, services: &CubeServices) -> Arc<dyn BenchState> {
        let dir = std::env::current_dir().unwrap().join("data");
        let path = dir.join("github-commits-000.csv");
        if !path.exists() {
            println!("Downloading github-commits-000.csv");
            let response = reqwest::get("https://media.githubusercontent.com/media/cube-js/testing-fixtures/master/github-commits-000.tar.gz").await.unwrap();
            let content =  Cursor::new(response.bytes().await.unwrap());
            let tarfile = GzDecoder::new(content);
            let mut archive = Archive::new(tarfile);
            archive.unpack(dir).unwrap();
        }
        assert!(path.exists());
        let _ = services.sql_service
            .exec_query("CREATE SCHEMA IF NOT EXISTS test")
            .await
            .unwrap();
        let _ = services.sql_service
            .exec_query(format!("CREATE TABLE test.table (`repo` text, `email` text, `commit_count` int) WITH (input_format = 'csv') LOCATION '{}'", path.to_str().unwrap()).as_str())
            .await
            .unwrap();

        let partitions = services.meta_store.partition_table().all_rows().await.unwrap();
        let scheduler = services.scheduler.clone();
        join_all(partitions.iter().map(|p| scheduler.schedule_partition_to_compact(&p))).await.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
        let jobs = partitions.iter().map(|p| (RowKey::Table(TableId::Partitions, p.get_id()), JobType::PartitionCompaction)).collect::<Vec<_>>();
        let listener = services.cluster.job_result_listener();
        timeout(Duration::from_secs(10), listener.wait_for_job_results(jobs)).await.unwrap().unwrap();

        let state = Arc::new(EmptyBenchState {});
        // warmup metadata cache
        self.bench(services, state.clone()).await;

        state
    }

    async fn bench(self: &Self, services: &CubeServices, _state: Arc<dyn BenchState>) {
        let repo = "2degrees/twod.wsgi";
        let r = services.sql_service.exec_query(format!("SELECT COUNT(*) FROM test.table WHERE repo = '{}' GROUP BY repo", repo).as_str()).await.unwrap();
        let rows = to_rows(&r);
        assert_eq!(rows, vec![vec![TableValue::Int(6)]]);
    }
}

pub fn cubestore_benches() -> Vec<Arc<dyn Bench>> {
    return vec![
        Arc::new(ParquetMetadataCacheBench {}),
    ]
}