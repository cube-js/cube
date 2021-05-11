//! Runs the SQL tests with a cluster that consists of 1 router and 2 select workers.
//! Note that each worker will also spawns 2 subprocesses for actual processing.
use async_trait::async_trait;
use cubestore::config::Config;
use cubestore_sql_tests::multiproc::{
    run_multiproc_test, MultiProcTest, SignalInit, WaitCompletion, WorkerProc,
};
use cubestore_sql_tests::{run_sql_tests, TestFn};
use serde_derive::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;

#[cfg(not(target_os = "windows"))]
fn main() {
    // Prepare workers.
    Config::configure_worker_services();
    procspawn::init(); // TODO: logs in worker processes.

    let port_counter = Arc::new(AtomicU16::new(0));
    const METASTORE_PORT: u16 = 51336;
    const WORKER_PORTS: [u16; 2] = [51337, 51338];

    run_sql_tests("cluster", Vec::new(), move |test_name, test_fn| {
        let port_shift = port_counter.fetch_add(10, Ordering::Relaxed);
        // Add a suffix to avoid clashes with other configurations run concurrently.
        // TODO: run each test in unique temp folder.
        run_multiproc_test(ClusterSqlTest {
            test_name: test_name.to_owned() + "-cluster",
            test_fn,
            port_shift,
        });

        #[derive(Serialize, Deserialize)]
        struct WorkerArgs {
            id: usize,
            test_name: String,
            port_shift: u16,
        }

        struct ClusterSqlTest {
            test_name: String,
            test_fn: TestFn,
            port_shift: u16,
        }

        #[async_trait]
        impl MultiProcTest for ClusterSqlTest {
            type WorkerArgs = WorkerArgs;
            type WorkerProc = WorkerFn;

            fn worker_arguments(&self) -> Vec<WorkerArgs> {
                (0..=1)
                    .map(|i| WorkerArgs {
                        test_name: self.test_name.clone(),
                        id: i,
                        port_shift: self.port_shift,
                    })
                    .collect()
            }

            async fn drive(self) {
                Config::test(&self.test_name)
                    .update_config(|mut c| {
                        c.server_name = format!("localhost:{}", METASTORE_PORT + self.port_shift);
                        c.metastore_bind_address = Some(c.server_name.clone());
                        c.select_workers = WORKER_PORTS
                            .iter()
                            .map(|p| format!("localhost:{}", p + self.port_shift))
                            .collect();
                        c
                    })
                    .start_test(|services| async move {
                        (self.test_fn)(Box::new(services.sql_service)).await;
                    })
                    .await;
            }
        }

        #[derive(Default)]
        struct WorkerFn;
        #[async_trait]
        impl WorkerProc<WorkerArgs> for WorkerFn {
            async fn run(
                self,
                WorkerArgs {
                    id,
                    test_name,
                    port_shift,
                }: WorkerArgs,
                init: SignalInit,
                done: WaitCompletion,
            ) {
                // Note that Rust's libtest does not consume output in subprocesses.
                // Disable logs to keep output compact.
                if !std::env::var("CUBESTORE_TEST_LOG_WORKER").is_ok() {
                    *cubestore::config::TEST_LOGGING_INITIALIZED.write().await = true;
                }
                let worker_addr = format!("localhost:{}", WORKER_PORTS[id] + port_shift);
                Config::test(&test_name)
                    .update_config(|mut c| {
                        c.select_worker_pool_size = 2;
                        c.server_name = worker_addr.to_string();
                        c.worker_bind_address = Some(c.server_name.clone());
                        c.metastore_remote_address =
                            Some(format!("localhost:{}", METASTORE_PORT + port_shift));
                        c
                    })
                    .start_test_worker(|_| async move {
                        // TODO ensures that port is ready and can be connected. Somehow waiting for bind isn't enough and socket isn't actually ready outside of a process?
                        TcpStream::connect(worker_addr.to_string()).await.unwrap();
                        init.signal().await;
                        done.wait_completion().await;
                    })
                    .await
            }
        }
    });
}

#[cfg(target_os = "windows")]
fn main() {
    // We do not procspawn on Windows.
}
