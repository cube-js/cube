// use async_trait::async_trait;
// use serde_derive::{Deserialize, Serialize};
// use std::future::Future;
// use criterion::{black_box, criterion_group, criterion_main, Criterion};
// use criterion::async_executor::AsyncExecutor;
// use cubestore::cluster;
// use cubestore::config::Config;
// use cubestore::util::respawn;
// use cubestore_sql_tests::multiproc::{multiproc_child_main, MultiProcTest, run_multiproc_test, Runtime, SignalInit, WaitCompletion, WorkerProc};
// use cubestore_sql_tests::{SqlClient, TestFn, to_rows};
//
// const METASTORE_PORT: u16 = 51336;
// const WORKER_PORTS: [u16; 2] = [51337, 51338];
//
// struct ClusterSqlBench<'c> {
//     criterion: &'c mut Criterion,
//     test_name: String,
//     test_fn: TestFn,
// }
//
// #[derive(Serialize, Deserialize)]
// struct WorkerArgs {
//     id: usize,
//     test_name: String,
// }
//
// #[derive(Default)]
// pub struct WorkerFn;
// #[async_trait]
// impl WorkerProc<WorkerArgs> for WorkerFn {
//     async fn run(
//         self,
//         WorkerArgs { id, test_name }: WorkerArgs,
//         init: SignalInit,
//         done: WaitCompletion,
//     ) {
//         // Note that Rust's libtest does not consume output in subprocesses.
//         // Disable logs to keep output compact.
//         if !std::env::var("CUBESTORE_TEST_LOG_WORKER").is_ok() {
//             *cubestore::config::TEST_LOGGING_INITIALIZED.write().await = true;
//         }
//         Config::test(&test_name)
//             .update_config(|mut c| {
//                 c.select_worker_pool_size = 2;
//                 c.server_name = format!("localhost:{}", WORKER_PORTS[id]);
//                 c.worker_bind_address = Some(c.server_name.clone());
//                 c.metastore_remote_address = Some(format!("localhost:{}", METASTORE_PORT));
//                 c
//             })
//             .start_test_worker(|_| async move {
//                 init.signal().await;
//                 done.wait_completion().await;
//             })
//             .await
//     }
// }
//
// #[async_trait]
// impl MultiProcTest for ClusterSqlBench<'_> {
//     type WorkerArgs = WorkerArgs;
//     type WorkerProc = WorkerFn;
//
//     fn worker_arguments(&self) -> Vec<WorkerArgs> {
//         (0..=1)
//             .map(|i| WorkerArgs {
//                 test_name: self.test_name.clone(),
//                 id: i,
//             })
//             .collect()
//     }
//
//     async fn drive(self) {
//         let config = Config::test(&self.test_name)
//             .update_config(|mut c| {
//                 c.server_name = format!("localhost:{}", METASTORE_PORT);
//                 c.metastore_bind_address = Some(c.server_name.clone());
//                 c.select_workers = WORKER_PORTS
//                     .iter()
//                     .map(|p| format!("localhost:{}", p))
//                     .collect();
//                 c
//             });
//         config
//             .start_test(|services| async move {
//                 self.criterion.bench_function(self.test_name.as_str(), |b| {
//                     let runtime = Runtime::new_current_thread().inner();
//                     b.to_async(runtime).iter(|| async {
//                         (self.test_fn)(Box::new(services.sql_service)).await;
//                     });
//                 });
//             })
//             .await;
//     }
// }
//
// fn t<F>(name: &'static str, f: fn(Box<dyn SqlClient>) -> F) -> (String, TestFn)
//     where
//         F: Future<Output = ()> + Send + 'static,
// {
//     (name.to_string(), Box::new(move |c| Box::pin(f(c))))
// }
//
// // pub fn benches() -> Vec<(String, TestFn)> {
// //     return vec![
// //         t("parquet_metadata_cache", parquet_metadata_cache),
// //     ];
// // }
//
// async fn parquet_metadata_cache(service: Box<dyn SqlClient>) {
//     let r = service.exec_query("SELECT 23").await.unwrap();
//     let rows = to_rows(&r);
//     println!("QQQ {:?}", rows);
// }
//
// fn cubestore_bench(criterion: &mut Criterion) {
//     respawn::register_handler(multiproc_child_main::<ClusterSqlBench>);
//     respawn::init(); // TODO: logs in worker processes.
//
//     let (test_name, test_fn) = t("parquet_metadata_cache", parquet_metadata_cache);
//     run_multiproc_test(ClusterSqlBench {criterion, test_name, test_fn})
// }
//
// criterion_group!(benches, cubestore_bench);
// criterion_main!(benches);
