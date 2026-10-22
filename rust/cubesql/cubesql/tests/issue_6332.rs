//! https://github.com/cube-js/cube/issues/6332
//! Subtracting an interval from a timestamp column of a subquery fails in the SQL API:
//!   Internal Error: Data type Timestamp(Nanosecond, None) not supported for
//!   scalar operation 'subtract' on primitive array
//! The same arithmetic on a literal, or on a cube column, succeeds.

use cubesql::{
    compile::{test::TestContext, DatabaseProtocol},
    CubeError,
};

#[tokio::test]
async fn issue_6332_literal_timestamp_minus_interval() -> Result<(), CubeError> {
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

    let result = context
        .execute_query(
            "SELECT * FROM (SELECT CAST('2023-03-03 00:00:00' AS timestamp) - INTERVAL '1 day' AS startdate) t",
        )
        .await?;
    assert!(result.contains("2023-03-02T00:00:00"), "{}", result);

    Ok(())
}

#[tokio::test]
async fn issue_6332_subquery_column_minus_interval() -> Result<(), CubeError> {
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

    let result = context
        .execute_query(
            "SELECT t.startdate - INTERVAL '1 day' AS prev FROM (SELECT CAST('2023-03-03 00:00:00' AS timestamp) AS startdate) t",
        )
        .await?;
    assert!(result.contains("2023-03-02T00:00:00"), "{}", result);

    Ok(())
}
