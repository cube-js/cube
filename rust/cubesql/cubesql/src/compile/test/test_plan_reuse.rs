use crate::{
    compile::{
        test::{utils::LogicalPlanTestUtils, TestContext},
        DatabaseProtocol,
    },
    sql::postgres::shim::AsyncPostgresShim,
    telemetry::SessionLogger,
    CubeError,
};
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tokio_util::sync::CancellationToken;

const QUERY: &str = "SELECT customer_gender, MEASURE(count) AS cnt \
    FROM KibanaSampleDataEcommerce GROUP BY 1 ORDER BY 1";

/// Serve one connection over the shim; the context records what the transport was asked.
async fn serve() -> (Arc<TestContext>, Client) {
    let context = Arc::new(TestContext::new(DatabaseProtocol::PostgreSQL).await);

    let request = context
        .convert_sql_to_cube_query(QUERY)
        .await
        .expect("the query must plan")
        .as_logical_plan()
        .find_cube_scan()
        .request;
    context
        .add_cube_load_mock(
            request,
            crate::compile::tests::simple_load_response(
                vec![
                    "KibanaSampleDataEcommerce.customer_gender",
                    "KibanaSampleDataEcommerce.count",
                ],
                vec![
                    vec![json!("female"), json!("male")],
                    vec![json!(2), json!(3)],
                ],
            ),
        )
        .await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("must bind a port");
    let port = listener.local_addr().unwrap().port();
    let session_manager = context.session.session_manager.clone();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("must accept a connection");
        let session = session_manager
            .create_session(
                DatabaseProtocol::PostgreSQL,
                "127.0.0.1".to_string(),
                1234,
                None,
            )
            .await
            .expect("must create a session");
        let logger = Arc::new(SessionLogger::new(session.state.clone()));

        AsyncPostgresShim::run_on(
            CancellationToken::new(),
            CancellationToken::new(),
            socket,
            session,
            logger,
        )
        .await
        .expect("connection must be handled");
    });

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test password=test", port),
        NoTls,
    )
    .await
    .expect("must connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    (context, client)
}

async fn query_rows(client: &Client, query: &str) -> Vec<String> {
    client
        .simple_query(query)
        .await
        .unwrap_or_else(|err| panic!("{} failed: {:?}", query, err))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).unwrap_or_default().to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

async fn planned_queries(context: &TestContext) -> usize {
    context
        .load_state_events()
        .await
        .iter()
        .filter(|event| event.as_str() == "SQL API Query Planning")
        .count()
}

#[tokio::test]
async fn test_cursor_plans_the_query_once() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    query_rows(&client, "BEGIN").await;
    query_rows(&client, &format!("DECLARE c CURSOR FOR {}", QUERY)).await;
    assert_eq!(planned_queries(&context).await, 1);

    // The first FETCH runs the plan DECLARE built, the next ones read the open portal
    assert_eq!(query_rows(&client, "FETCH 1 IN c").await, vec!["female|2"]);
    assert_eq!(query_rows(&client, "FETCH 1 IN c").await, vec!["male|3"]);
    assert_eq!(
        query_rows(&client, "FETCH 1 IN c").await,
        Vec::<String>::new()
    );
    query_rows(&client, "CLOSE c").await;
    query_rows(&client, "COMMIT").await;

    assert_eq!(planned_queries(&context).await, 1);
    assert_eq!(context.load_calls().await.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_cursor_with_hold_plans_the_query_once() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    query_rows(&client, "BEGIN").await;
    query_rows(
        &client,
        &format!("DECLARE c CURSOR WITH HOLD FOR {}", QUERY),
    )
    .await;
    query_rows(&client, "COMMIT").await;

    // A cursor declared WITH HOLD outlives the transaction, and so does its plan
    assert_eq!(
        query_rows(&client, "FETCH ALL IN c").await,
        vec!["female|2", "male|3"]
    );
    query_rows(&client, "CLOSE c").await;

    // A cursor declared again under the same name is planned again
    query_rows(&client, "BEGIN").await;
    query_rows(&client, &format!("DECLARE c CURSOR FOR {}", QUERY)).await;
    assert_eq!(
        query_rows(&client, "FETCH ALL IN c").await,
        vec!["female|2", "male|3"]
    );
    query_rows(&client, "COMMIT").await;

    assert_eq!(planned_queries(&context).await, 2);
    assert_eq!(context.load_calls().await.len(), 2);

    Ok(())
}

async fn extended_rows(client: &Client, statement: &tokio_postgres::Statement) -> Vec<String> {
    client
        .query(statement, &[])
        .await
        .expect("the statement must run")
        .iter()
        .map(|row| format!("{}|{}", row.get::<_, String>(0), row.get::<_, i64>(1)))
        .collect()
}

#[tokio::test]
async fn test_extended_query_plans_the_query_once() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    // Parse plans the query to describe its columns; Bind runs that plan
    let statement = client.prepare(QUERY).await.expect("the query must parse");
    assert_eq!(planned_queries(&context).await, 1);
    assert_eq!(
        extended_rows(&client, &statement).await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 1);

    // The Parse plan serves one Bind: binding the statement again plans it again
    assert_eq!(
        extended_rows(&client, &statement).await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 2);
    assert_eq!(context.load_calls().await.len(), 2);

    Ok(())
}

#[tokio::test]
async fn test_extended_query_after_user_change_plans_again() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    let statement = client.prepare(QUERY).await.expect("the query must parse");
    query_rows(&client, "SET user = 'good_user'").await;

    // The Parse plan belongs to the previous user, so Bind plans the query for the new one
    assert_eq!(
        extended_rows(&client, &statement).await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 2);

    Ok(())
}

#[tokio::test]
async fn test_cursor_after_user_change_plans_again() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    query_rows(&client, "BEGIN").await;
    query_rows(
        &client,
        &format!("DECLARE c CURSOR WITH HOLD FOR {}", QUERY),
    )
    .await;
    query_rows(&client, "COMMIT").await;
    query_rows(&client, "SET user = 'good_user'").await;

    // The DECLARE plan belongs to the previous user, so FETCH plans the query for the new one
    assert_eq!(
        query_rows(&client, "FETCH ALL IN c").await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 2);

    Ok(())
}

#[tokio::test]
async fn test_cursor_fetch_is_the_data_request() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    query_rows(&client, "BEGIN").await;
    query_rows(&client, &format!("DECLARE c CURSOR FOR {}", QUERY)).await;
    query_rows(&client, "FETCH ALL IN c").await;
    query_rows(&client, "COMMIT").await;

    // FETCH runs the plan DECLARE built without planning, yet it loads the data
    let fetch_is_data_query = context
        .load_state_events_with_properties()
        .await
        .into_iter()
        .filter(|(event, properties)| {
            event == "Load Request Success"
                && properties["query"].to_string().contains("FETCH ALL IN c")
        })
        .map(|(_, properties)| properties["isDataQuery"].clone())
        .collect::<Vec<_>>();
    assert_eq!(fetch_is_data_query, vec![json!(true)]);

    Ok(())
}
