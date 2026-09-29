//! https://github.com/cube-js/cube/issues/5512
//! Chartbrew (Sequelize, postgres dialect) cannot connect to the SQL API.
//!
//! On connect, Sequelize sends:
//!   SET client_min_messages TO warning;SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE;
//! (the INTERVAL form is used whenever `timezone` is an offset such as the default '+00:00').
//! Postgres accepts `SET TIME ZONE INTERVAL '<offset>' HOUR TO MINUTE`; the SQL API rejects it.

use cubesql::{
    compile::{test::TestContext, DatabaseProtocol},
    CubeError,
};

#[tokio::test]
async fn issue_5512_sequelize_set_time_zone_interval() -> Result<(), CubeError> {
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

    context
        .execute_queries_with_flags(vec!["SET client_min_messages TO warning"])
        .await?;

    context
        .execute_queries_with_flags(vec!["SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE"])
        .await?;

    Ok(())
}

#[tokio::test]
async fn issue_5512_sequelize_set_time_zone_non_utc_interval() -> Result<(), CubeError> {
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;

    context
        .execute_queries_with_flags(vec!["SET TIME ZONE INTERVAL '+02:00' HOUR TO MINUTE"])
        .await?;

    Ok(())
}
