#![feature(async_closure)]
#![feature(test)]

pub use crate::benches::cubestore_benches;
pub use crate::tests::{to_rows, TestFn};
extern crate test;
use async_trait::async_trait;
use cubestore::sql::{QueryPlans, SqlQueryContext, SqlService};
use cubestore::store::DataFrame;
use cubestore::CubeError;
use std::env;
use std::panic::RefUnwindSafe;
use std::sync::Arc;
use test::TestFn::DynTestFn;
use test::{ShouldPanic, TestDesc, TestDescAndFn, TestName, TestType};
use tests::sql_tests;

mod benches;
pub mod files;
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
    fn prefix(&self) -> &str;
    /// Used by FilterWritesSqlClient in migration tests, ignored for others.
    fn migration_run_next_query(&self) {}
    /// Used by FilterWritesSqlClient in migration tests, ignored for others.
    fn migration_hardcode_next_query(&self, _next_result: Result<Arc<DataFrame>, CubeError>) {}
}

impl dyn SqlClient {
    /// Use this instead of prefix() so that other uses of prefix() are easily searchable and
    /// enumerable.
    fn is_migration(&self) -> bool {
        self.prefix() == "migration"
    }

    /// Doesn't do anything but is a searchable token for later test management.
    fn note_non_idempotent_migration_test(&self) {}

    /// We tolerate the next query but we want to revisit later because maybe it should be a rule in
    /// the FilterWritesSqlClient's recognized queries list.
    fn tolerate_next_query_revisit(&self) {
        self.migration_run_next_query()
    }

    /// Hardcodes an error return value, for when the presence of an error but not the message is asserted.
    fn migration_hardcode_generic_err(&self) {
        self.migration_hardcode_next_query(Err(CubeError::user(String::new())));
    }
}

pub fn run_sql_tests(
    prefix: &str,
    extra_args: Vec<String>,
    runner: impl Fn(/*test_name*/ &str, TestFn) + RefUnwindSafe + Send + Sync + Clone + 'static,
) {
    let tests = sql_tests(prefix)
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

    test::test_main(
        &env::args().chain(extra_args).collect::<Vec<String>>(),
        tests,
        None,
    );
}

pub struct BasicSqlClient {
    /// Used rarely in some test cases, or maybe frequently for the "migration" prefix.
    pub prefix: &'static str,
    pub service: Arc<dyn SqlService>,
}

#[async_trait]
impl SqlClient for BasicSqlClient {
    async fn exec_query(&self, query: &str) -> Result<Arc<DataFrame>, CubeError> {
        self.service.as_ref().exec_query(query).await
    }

    async fn exec_query_with_context(
        &self,
        context: SqlQueryContext,
        query: &str,
    ) -> Result<Arc<DataFrame>, CubeError> {
        self.service
            .as_ref()
            .exec_query_with_context(context, query)
            .await
    }

    async fn plan_query(&self, query: &str) -> Result<QueryPlans, CubeError> {
        self.service.as_ref().plan_query(query).await
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}
