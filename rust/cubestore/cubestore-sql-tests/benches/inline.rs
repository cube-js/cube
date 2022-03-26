use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use std::future::Future;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use criterion::async_executor::AsyncExecutor;
use tokio::runtime::Builder;
use cubestore::cluster;
use cubestore::config::Config;
use cubestore::util::respawn;
use cubestore_sql_tests::multiproc::{multiproc_child_main, MultiProcTest, run_multiproc_test, Runtime, SignalInit, WaitCompletion, WorkerProc};
use cubestore_sql_tests::{SqlClient, TestFn, to_rows};

fn t<F>(name: &'static str, f: fn(Box<dyn SqlClient>) -> F) -> (String, TestFn)
    where
        F: Future<Output = ()> + Send + 'static,
{
    (name.to_string(), Box::new(move |c| Box::pin(f(c))))
}

async fn parquet_metadata_cache(service: Box<dyn SqlClient>) {
    let r = service.exec_query("SELECT 23").await.unwrap();
    let rows = to_rows(&r);
    println!("QQQ {:?}", rows);
}

fn inline_bench(criterion: &mut Criterion) {
    let (test_name, test_fn) = t("parquet_metadata_cache", parquet_metadata_cache);
    
    criterion.bench_function(test_name.as_str(), |b| {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        b.to_async(runtime).iter(|| async {
            Config::run_test(test_name.as_str(), |services| async move {
                (test_fn)(Box::new(services.sql_service)).await;
            });
        });
    });
}

criterion_group!(benches, inline_bench);
criterion_main!(benches);
