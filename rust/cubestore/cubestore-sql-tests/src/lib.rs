#![feature(test)]

pub use crate::benches::cubestore_benches;
pub use crate::tests::{to_rows, TestFn};
extern crate test;
use async_trait::async_trait;
use cubestore::sql::{QueryPlans, SqlQueryContext, SqlService};
use cubestore::store::DataFrame;
use cubestore::CubeError;
use std::env::args;
use std::panic::RefUnwindSafe;
use std::sync::Arc;
use test::TestFn::DynTestFn;
use test::{ShouldPanic, TestDesc, TestDescAndFn, TestName, TestType};
use tests::sql_tests;

mod benches;
mod files;
#[cfg(not(target_os = "windows"))]
pub mod multiproc;
#[allow(unused_parens, non_snake_case)]
mod rows;
mod tests;

#[async_trait]
pub trait SqlClient: Send + Sync {
    async fn exec_query(&self, query: &str) -> Result<Arc<DataFrame>, CubeError>;
    async fn exec_query_with_context(
        &self,
        context: SqlQueryContext,
        query: &str,
    ) -> Result<Arc<DataFrame>, CubeError>;
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
                    ignore_message: None,
                    source_file: "",
                    start_line: 0,
                    start_col: 0,
                    end_line: 0,
                    compile_fail: false,
                    no_run: false,
                    test_type: TestType::IntegrationTest,
                    end_col: 0,
                },
                testfn: DynTestFn(Box::new(move || {
                    runner(name, test_fn);
                    Ok(())
                })),
            }
        })
        .collect();

    test::test_main(&merge_args(args().collect(), extra_args), tests, None);
}

fn merge_args(mut base: Vec<String>, extra: Vec<String>) -> Vec<String> {
    for extra_arg in extra.into_iter() {
        if let Some((arg_name, _arg_value)) = extra_arg.split_once('=') {
            base.retain(|a| !a.starts_with(arg_name));
        }

        base.push(extra_arg.to_string());
    }

    base
}

#[async_trait]
impl SqlClient for Arc<dyn SqlService> {
    async fn exec_query(&self, query: &str) -> Result<Arc<DataFrame>, CubeError> {
        self.as_ref().exec_query(query).await
    }

    async fn exec_query_with_context(
        &self,
        context: SqlQueryContext,
        query: &str,
    ) -> Result<Arc<DataFrame>, CubeError> {
        self.as_ref().exec_query_with_context(context, query).await
    }

    async fn plan_query(&self, query: &str) -> Result<QueryPlans, CubeError> {
        self.as_ref().plan_query(query).await
    }
}

#[cfg(test)]
mod test_helpers {
    use super::*;

    #[test]
    fn test_merge_args() {
        let base = vec![
            "path/to/executable".to_string(),
            "--test-threads=1".to_string(),
        ];
        let extra = vec![
            "--test-threads=2".to_string(),
            "--skip=planning_inplace_aggregate2".to_string(),
        ];

        let merged = merge_args(base, extra);
        assert_eq!(
            merged,
            vec![
                "path/to/executable",
                "--test-threads=2",
                "--skip=planning_inplace_aggregate2"
            ]
        );
    }
}
