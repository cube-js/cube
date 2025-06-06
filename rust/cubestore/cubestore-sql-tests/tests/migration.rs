//! Runs SQL tests in a single process, using the previous version of Cubestore instance, to test forward migration.
use std::{env, ops::DerefMut as _, path::Path, sync::Arc};

use async_trait::async_trait;
use cubestore::{
    config::Config,
    sql::{QueryPlans, SqlQueryContext, SqlService},
    store::DataFrame,
    CubeError,
};
use cubestore_sql_tests::{files::recursive_copy_directory, run_sql_tests, SqlClient};
use tokio::runtime::Builder;

fn main() {
    let migration_test_dirs: Box<Path> = {
        let r = Builder::new_current_thread().enable_all().build().unwrap();

        r.block_on(
            cubestore_sql_tests::files::download_and_unzip(
                "https://github.com/cube-js/testing-fixtures/raw/master/cubestore_migration_test_directories_0001.tar.gz",
                "migration-test-dirs",
            )).unwrap()
    };

    run_sql_tests("migration", vec![], move |test_name, test_fn| {
        let r = Builder::new_current_thread()
            .thread_stack_size(4 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();
        // Add a suffix to avoid clashes with other configurations run concurrently.  (This suffix
        // is used the migration tarball's directory names, which were renamed from in_process's
        // "-1p" suffix.)
        // TODO: run each test in unique temp folder.
        let test_name = test_name.to_owned() + "-migration";

        {
            let from_dir = Config::test_data_dir_path(&migration_test_dirs, &test_name);
            let to_dir = Config::test_data_dir_path(&env::current_dir().unwrap(), &test_name);
            if let Err(e) = recursive_copy_directory(&from_dir, &to_dir) {
                panic!(
                    "could not copy data directory from {:?} to {:?}: {}",
                    from_dir, to_dir, e
                );
            }
        }
        {
            let from_dir = Config::test_remote_dir_path(&migration_test_dirs, &test_name);
            if std::fs::exists(&from_dir).unwrap() {
                let to_dir = Config::test_remote_dir_path(&env::current_dir().unwrap(), &test_name);
                if let Err(e) = recursive_copy_directory(&from_dir, &to_dir) {
                    panic!(
                        "could not copy 'remote' directory from {:?} to {:?}: {}",
                        from_dir, to_dir, e
                    );
                }
            }
        }

        r.block_on(Config::run_migration_test(
            &test_name,
            |services| async move {
                test_fn(Box::new(FilterWritesSqlClient::new(services.sql_service))).await;
            },
        ));
    });
}

enum NextQueryTreatment {
    FilterNormally,
    AlwaysAllow,
    Hardcoded(Result<Arc<DataFrame>, CubeError>),
}

struct FilterWritesSqlClient {
    // An AtomicBool simply because `SqlClient: Send + Sync` and has an immutable API.
    tolerate_next_query_flag: std::sync::Mutex<NextQueryTreatment>,
    sql_service: Arc<dyn SqlService>,
}

impl FilterWritesSqlClient {
    fn new(sql_service: Arc<dyn SqlService>) -> FilterWritesSqlClient {
        FilterWritesSqlClient {
            tolerate_next_query_flag: std::sync::Mutex::new(NextQueryTreatment::FilterNormally),
            sql_service,
        }
    }

    fn replace_tolerate_next_query_flag(
        &self,
        new_flag_value: NextQueryTreatment,
    ) -> NextQueryTreatment {
        let mut guard = self
            .tolerate_next_query_flag
            .lock()
            .expect("unpoisoned tolerate_next_query_flag");
        std::mem::replace(guard.deref_mut(), new_flag_value)
    }
}

enum FilterQueryResult {
    RunQuery,
    Hardcoded(Result<Arc<DataFrame>, CubeError>),
    UnrecognizedQueryType,
}

impl FilterWritesSqlClient {
    fn should_filter(query: &str) -> FilterQueryResult {
        let q = query.trim_ascii_start().to_ascii_lowercase();

        let should_skip =
            q.starts_with("insert ") || q.starts_with("create ") || q.starts_with("cache set ");

        if should_skip {
            return FilterQueryResult::Hardcoded(Ok(Arc::new(DataFrame::new(vec![], vec![]))));
        }

        let recognized = q.starts_with("select ")
            || q.starts_with("select\n")
            || q.starts_with("cache get ")
            || q.starts_with("cache keys ")
            || q.starts_with("explain ")
            || q.starts_with("queue ");

        return if recognized {
            FilterQueryResult::RunQuery
        } else {
            FilterQueryResult::UnrecognizedQueryType
        };
    }

    /// Uses self's tolerate_next_query atomic bool, and sets it back to false.
    fn compute_filter_flag(&self, query: &str) -> FilterQueryResult {
        let flag = self.replace_tolerate_next_query_flag(NextQueryTreatment::FilterNormally);

        match flag {
            NextQueryTreatment::FilterNormally => Self::should_filter(query),
            NextQueryTreatment::AlwaysAllow => FilterQueryResult::RunQuery,
            NextQueryTreatment::Hardcoded(result) => FilterQueryResult::Hardcoded(result),
        }
    }
}

#[async_trait]
impl SqlClient for FilterWritesSqlClient {
    async fn exec_query(&self, query: &str) -> Result<Arc<DataFrame>, CubeError> {
        match self.compute_filter_flag(query) {
            FilterQueryResult::RunQuery => self.sql_service.exec_query(query).await,
            FilterQueryResult::Hardcoded(result) => result,
            FilterQueryResult::UnrecognizedQueryType => unimplemented!(
                "FilterWritesSqlClient does not support query prefix for '{}'",
                query
            ),
        }
    }
    async fn exec_query_with_context(
        &self,
        context: SqlQueryContext,
        query: &str,
    ) -> Result<Arc<DataFrame>, CubeError> {
        match self.compute_filter_flag(query) {
            FilterQueryResult::RunQuery => {
                self.sql_service
                    .exec_query_with_context(context, query)
                    .await
            }
            FilterQueryResult::Hardcoded(result) => result,
            FilterQueryResult::UnrecognizedQueryType => unimplemented!(
                "FilterWritesSqlClient does not support query prefix for '{}'",
                query
            ),
        }
    }
    async fn plan_query(&self, query: &str) -> Result<QueryPlans, CubeError> {
        self.sql_service.plan_query(query).await
    }

    fn prefix(&self) -> &str {
        "migration"
    }

    fn migration_run_next_query(&self) {
        let old_flag = self.replace_tolerate_next_query_flag(NextQueryTreatment::AlwaysAllow);
        assert!(matches!(old_flag, NextQueryTreatment::FilterNormally));
    }

    fn migration_hardcode_next_query(&self, next_result: Result<Arc<DataFrame>, CubeError>) {
        let old_flag =
            self.replace_tolerate_next_query_flag(NextQueryTreatment::Hardcoded(next_result));
        assert!(matches!(old_flag, NextQueryTreatment::FilterNormally));
    }
}
