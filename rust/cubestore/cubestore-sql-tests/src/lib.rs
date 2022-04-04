#![feature(async_closure)]
#![feature(test)]

pub use crate::tests::TestFn;
extern crate test;
use async_trait::async_trait;
use cubestore::sql::{QueryPlans, SqlService};
use cubestore::store::DataFrame;
use cubestore::CubeError;
use std::env;
use std::panic::RefUnwindSafe;
use std::sync::Arc;
use test::TestFn::DynTestFn;
use test::{ShouldPanic, TestDesc, TestDescAndFn, TestName, TestType};
use tests::sql_tests;

mod files;
#[cfg(not(target_os = "windows"))]
pub mod multiproc;
#[allow(unused_parens, non_snake_case)]
mod rows;
mod tests;

#[async_trait]
pub trait SqlClient: Send + Sync {
    async fn exec_query(&self, query: &str) -> Result<Arc<DataFrame>, CubeError>;
    async fn plan_query(&self, query: &str) -> Result<QueryPlans, CubeError>;
}

pub fn run_sql_tests(
    prefix: &str,
    extra_args: Vec<String>,
    runner: impl Fn(/*test_name*/ &str, TestFn) + RefUnwindSafe + Send + Sync + Clone + 'static,
) {
    let tests = sql_tests()
        .into_iter()
        .map(|(name, test_fn)| {
            let runner = runner.clone();
            TestDescAndFn {
                desc: TestDesc {
                    name: TestName::DynTestName(format!("cubesql::{}::{}", prefix, name)),
                    ignore: false,
                    should_panic: ShouldPanic::No,
                    allow_fail: false,
                    compile_fail: false,
                    no_run: false,
                    test_type: TestType::IntegrationTest,
                },
                testfn: DynTestFn(Box::new(move || runner(name, test_fn))),
            }
        })
        .collect();

    test::test_main(
        &env::args().chain(extra_args).collect::<Vec<String>>(),
        tests,
        None,
    );
}

#[async_trait]
impl SqlClient for Arc<dyn SqlService> {
    async fn exec_query(&self, query: &str) -> Result<Arc<DataFrame>, CubeError> {
        self.as_ref().exec_query(query).await
    }

    async fn plan_query(&self, query: &str) -> Result<QueryPlans, CubeError> {
        self.as_ref().plan_query(query).await
    }
}
