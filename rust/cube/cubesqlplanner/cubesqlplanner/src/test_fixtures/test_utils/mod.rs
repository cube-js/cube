mod test_context;

#[cfg(feature = "integration-cubestore")]
pub(crate) mod cubestore_service;
#[cfg(feature = "integration-postgres")]
pub(crate) mod integration_context;
#[cfg(feature = "integration-postgres")]
pub(crate) mod pg_service;

pub use test_context::TestContext;

use crate::planner::{CubeId, MemberId};

/// Runs plain SQL over a freshly seeded database, as an oracle for planned
/// queries; `None` unless built with `integration-postgres`.
#[cfg(feature = "integration-postgres")]
pub async fn try_execute_raw_pg(sql: &str, seed_file: &str) -> Option<String> {
    let client = pg_service::connect_and_seed(seed_file).await;
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL execution failed:\n{sql}\n\nError: {e:?}"));
    Some(integration_context::format_simple_query_results(&messages))
}

#[cfg(not(feature = "integration-postgres"))]
pub async fn try_execute_raw_pg(_sql: &str, _seed_file: &str) -> Option<String> {
    None
}

/// The id a member renders as `cube.member`, or `expr:cube.member` for an
/// expression.
pub fn member_id(full_name: &str) -> MemberId {
    if let Some(expression) = full_name.strip_prefix("expr:") {
        let (cube, name) = expression.split_once('.').unwrap();
        MemberId::expression(CubeId::cube(cube), name)
    } else {
        let (cube, name) = full_name.split_once('.').unwrap();
        MemberId::member(CubeId::cube(cube), name)
    }
}

/// Cells of a result table without its header, so results that differ only
/// in column aliases compare equal.
pub fn table_rows(result: &str) -> Vec<Vec<String>> {
    result
        .lines()
        .skip(2)
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.split('|')
                .map(|cell| cell.trim().to_string())
                .collect()
        })
        .collect()
}
