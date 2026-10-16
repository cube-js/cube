//! Regression test for https://github.com/cube-js/cube/issues/10307
//!
//! Postgrex/Ecto bootstrap reads `typsend`/`typoutput` from `pg_type` and
//! fails when they are NULL. In PostgreSQL these `regproc` columns are never
//! NULL (e.g. `boolsend` / `boolout`).

use crate::compile::{
    test::{execute_query, init_testing_logger},
    DatabaseProtocol,
};

#[tokio::test]
async fn test_issue_10307_pg_type_typsend_typoutput_not_null() -> Result<(), crate::CubeError> {
    init_testing_logger();

    let result = execute_query(
        "SELECT count(*) AS null_cnt FROM pg_catalog.pg_type t \
         WHERE t.typsend IS NULL OR t.typoutput IS NULL"
            .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await?;

    assert_eq!(
        result, "+----------+\n| null_cnt |\n+----------+\n| 0        |\n+----------+",
        "pg_type.typsend / pg_type.typoutput must not be NULL"
    );

    Ok(())
}
