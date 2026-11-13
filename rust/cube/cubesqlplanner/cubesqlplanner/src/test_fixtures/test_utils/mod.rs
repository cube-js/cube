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

/// Rows of a result without the named column, sorted, so results that differ
/// only in that column and in row order compare equal.
pub fn rows_without_column(result: &str, column: &str) -> Vec<Vec<String>> {
    let header: Vec<String> = result
        .lines()
        .next()
        .unwrap_or_default()
        .split('|')
        .map(|cell| cell.trim().to_string())
        .collect();
    let skip = header.iter().position(|name| name == column);
    let mut rows: Vec<Vec<String>> = table_rows(result)
        .into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .filter(|(i, _)| Some(*i) != skip)
                .map(|(_, cell)| cell)
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

/// Asserts on Postgres that `query` returns the rows of `equivalent`, a query
/// computing the same values another way, with the named columns left out.
pub async fn assert_same_rows(
    ctx: &TestContext,
    seed_file: &str,
    query: &str,
    column: &str,
    equivalent: &str,
    equivalent_column: &str,
) {
    let (Some(result), Some(expected)) = (
        ctx.try_execute_pg(query, seed_file).await,
        ctx.try_execute_pg(equivalent, seed_file).await,
    ) else {
        return;
    };
    assert_eq!(
        rows_without_column(&result, column),
        rows_without_column(&expected, equivalent_column),
        "\nresult:\n{}\nexpected:\n{}",
        result,
        expected
    );
}
