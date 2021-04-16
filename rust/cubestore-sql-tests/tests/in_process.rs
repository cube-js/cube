//! Runs SQL tests in a single process.
use cubestore::config::Config;
use cubestore_sql_tests::run_sql_tests;
use tokio::runtime::Builder;

fn main() {
    run_sql_tests("in_process", vec![], |test_name, test_fn| {
        let r = Builder::new_current_thread().enable_all().build().unwrap();
        // Add a suffix to avoid clashes with other configurations run concurrently.
        // TODO: run each test in unique temp folder.
        let test_name = test_name.to_owned() + "-1p";
        r.block_on(Config::run_test(&test_name, |services| async move {
            test_fn(Box::new(services.sql_service)).await;
        }));
    });
}
