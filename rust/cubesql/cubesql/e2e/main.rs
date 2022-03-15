use std::fmt::Debug;

use cubesql::telemetry::ReportingLogger;
use log::Level;
use simple_logger::SimpleLogger;
use tests::{
    basic::{AsyncTestConstructorResult, AsyncTestSuite},
    mysql::MySqlIntegrationTestSuite,
    postgres::PostgresIntegrationTestSuite,
};

pub mod tests;

#[derive(Debug)]
struct TestsRunner {
    pub suites: Vec<Box<dyn AsyncTestSuite>>,
}

impl TestsRunner {
    pub fn new() -> Self {
        Self { suites: Vec::new() }
    }

    pub fn register_suite(&mut self, result: AsyncTestConstructorResult) {
        match result {
            AsyncTestConstructorResult::Sucess(suite) => self.suites.push(suite),
            AsyncTestConstructorResult::Skipped(message) => {
                println!("Skipped: {}", message)
            }
        }
    }
}

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let log_level = Level::Trace;

    let logger = SimpleLogger::new()
        .with_level(Level::Error.to_level_filter())
        .with_module_level("cubeclient", log_level.to_level_filter())
        .with_module_level("cubesql", log_level.to_level_filter());

    ReportingLogger::init(Box::new(logger), log_level.to_level_filter()).unwrap();

    rt.block_on(async {
        let mut runner = TestsRunner::new();
        runner.register_suite(MySqlIntegrationTestSuite::before_all().await);
        runner.register_suite(PostgresIntegrationTestSuite::before_all().await);

        for suites in runner.suites.iter_mut() {
            suites.run().await.unwrap();
        }
    });
}
