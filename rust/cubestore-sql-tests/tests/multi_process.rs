//! Runs the SQL tests with 2 select worker processes.
use cubestore::config::Config;
use cubestore_sql_tests::run_sql_tests;
use tokio::runtime::Builder;

#[cfg(not(target_os = "windows"))]
fn main() {
    // Prepare workers.
    Config::configure_worker_services();
    procspawn::init(); // TODO: logs on workers.

    run_sql_tests("multi_process", |test_name, test_fn| {
        let r = Builder::new_current_thread().enable_all().build().unwrap();
        // Add a suffix to avoid clashes with other configurations run concurrently.
        // TODO: run each test in unique temp folder.
        let test_name = test_name.to_owned() + "-2w";
        r.block_on(
            Config::test(&test_name)
                .update_config(|mut c| {
                    c.select_worker_pool_size = 2;
                    c
                })
                .start_test(|services| async move {
                    test_fn(Box::new(services.sql_service)).await;
                }),
        );
    });
}

#[cfg(target_os = "windows")]
fn main() {
    // We do not procspawn on Windows.
}
