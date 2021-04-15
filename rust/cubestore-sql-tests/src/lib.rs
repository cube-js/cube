#![feature(async_closure)]
#![feature(test)]

extern crate test;
use std::env;
use test::TestFn::DynTestFn;
use test::{ShouldPanic, TestDesc, TestDescAndFn, TestName, TestType};
use tests::sql_tests;

mod tests;

pub fn run_sql_tests(prefix: &str) {
    let tests = sql_tests()
        .into_iter()
        .map(|(name, test_fn)| TestDescAndFn {
            desc: TestDesc {
                name: TestName::DynTestName(format!("cubesql::{}::{}", prefix, name)),
                ignore: false,
                should_panic: ShouldPanic::No,
                allow_fail: false,
                test_type: TestType::IntegrationTest,
            },
            testfn: DynTestFn(Box::new(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(test_fn());
            })),
        })
        .collect();

    test::test_main(&env::args().collect::<Vec<String>>(), tests, None);
}
