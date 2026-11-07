use crate::{
    compile::{
        test::{utils::LogicalPlanTestUtils, TestContext},
        DatabaseProtocol,
    },
    sql::postgres::{extended::PLAN_REUSE_WINDOW, shim::AsyncPostgresShim},
    telemetry::SessionLogger,
    CubeError,
};
use cubeclient::models::V1LoadRequestQuery;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tokio_util::sync::CancellationToken;

const QUERY: &str = "SELECT customer_gender, MEASURE(count) AS cnt \
    FROM KibanaSampleDataEcommerce GROUP BY 1 ORDER BY 1";
const NEW_YORK: &str = "America/New_York";

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
    // The same rows answer the query planned after `SET TIME ZONE`, which carries the zone
    let in_new_york = V1LoadRequestQuery {
        timezone: Some(NEW_YORK.to_string()),
        ..request.clone()
    };
    for request in [request, in_new_york] {
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
    }

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

#[tokio::test]
async fn test_extended_query_after_recompile_plans_again() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    let statement = client.prepare(QUERY).await.expect("the query must parse");
    context.recompile_model();

    // The Parse plan belongs to the previous model, so Bind plans the query for the new one
    assert_eq!(
        extended_rows(&client, &statement).await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 2);

    Ok(())
}

#[tokio::test]
async fn test_cursor_after_recompile_plans_again() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    query_rows(&client, "BEGIN").await;
    query_rows(
        &client,
        &format!("DECLARE c CURSOR WITH HOLD FOR {}", QUERY),
    )
    .await;
    query_rows(&client, "COMMIT").await;
    context.recompile_model();

    // The DECLARE plan belongs to the previous model, so FETCH plans the query for the new one
    assert_eq!(
        query_rows(&client, "FETCH ALL IN c").await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 2);

    Ok(())
}

#[tokio::test]
async fn test_extended_query_after_set_plans_again() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    let statement = client.prepare(QUERY).await.expect("the query must parse");
    query_rows(&client, &format!("SET TIME ZONE '{}'", NEW_YORK)).await;

    // The Parse plan was built under the previous settings, so Bind plans the query again
    assert_eq!(
        extended_rows(&client, &statement).await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 2);

    Ok(())
}

#[tokio::test]
async fn test_cursor_after_set_plans_again() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    query_rows(&client, "BEGIN").await;
    query_rows(
        &client,
        &format!("DECLARE c CURSOR WITH HOLD FOR {}", QUERY),
    )
    .await;
    query_rows(&client, "COMMIT").await;
    query_rows(&client, &format!("SET TIME ZONE '{}'", NEW_YORK)).await;

    // The DECLARE plan was built under the previous settings, so FETCH plans the query again
    assert_eq!(
        query_rows(&client, "FETCH ALL IN c").await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 2);

    Ok(())
}

#[tokio::test]
async fn test_extended_query_bound_later_plans_again() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    let statement = client.prepare(QUERY).await.expect("the query must parse");
    tokio::time::sleep(PLAN_REUSE_WINDOW + Duration::from_millis(100)).await;

    // The Parse plan folded the clock in when it was built, so a later Bind plans again
    assert_eq!(
        extended_rows(&client, &statement).await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 2);

    Ok(())
}

#[tokio::test]
async fn test_cursor_fetched_later_plans_again() -> Result<(), CubeError> {
    let (context, client) = serve().await;

    query_rows(&client, "BEGIN").await;
    query_rows(
        &client,
        &format!("DECLARE c CURSOR WITH HOLD FOR {}", QUERY),
    )
    .await;
    query_rows(&client, "COMMIT").await;
    tokio::time::sleep(PLAN_REUSE_WINDOW + Duration::from_millis(100)).await;

    // The DECLARE plan folded the clock in when it was built, so a later FETCH plans again
    assert_eq!(
        query_rows(&client, "FETCH ALL IN c").await,
        vec!["female|2", "male|3"]
    );
    assert_eq!(planned_queries(&context).await, 2);

    Ok(())
}

#[tokio::test]
async fn test_dropped_session_releases_unbound_statements() -> Result<(), CubeError> {
    let context = Arc::new(TestContext::new(DatabaseProtocol::PostgreSQL).await);
    let session_manager = context.session.session_manager.clone();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("must bind a port");
    let port = listener.local_addr().unwrap().port();
    let session = session_manager
        .create_session(
            DatabaseProtocol::PostgreSQL,
            "127.0.0.1".to_string(),
            1234,
            None,
        )
        .await
        .expect("must create a session");
    let connection_id = session.state.connection_id;
    let state = Arc::downgrade(&session.state);

    let shim = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("must accept a connection");
        let logger = Arc::new(SessionLogger::new(session.state.clone()));
        let _ = AsyncPostgresShim::run_on(
            CancellationToken::new(),
            CancellationToken::new(),
            socket,
            session,
            logger,
        )
        .await;
    });

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test password=test", port),
        NoTls,
    )
    .await
    .expect("must connect");
    let connection = tokio::spawn(connection);

    // Parse keeps a plan for the statement, which is never bound: the connection goes away
    // while the statement is still open, so the client never closes it
    let statement = client.prepare(QUERY).await.expect("the query must parse");
    connection.abort();
    let _ = connection.await;
    shim.await.expect("the shim must stop");
    drop(statement);
    drop(client);

    session_manager.drop_session(connection_id).await;

    // The kept plan refers back to the session through its UDFs, so it must not outlive the
    // connection. (The rewrite cache may still hold the session until it evicts the query.)
    if let Some(state) = state.upgrade() {
        assert!(state.statements.read().await.is_empty());
    }

    Ok(())
}

/// The values of the filters on `member` in the SQL requests, as text.
async fn filter_values(context: &TestContext, member: &str) -> Vec<String> {
    let mut values = vec![];
    for call in context.sql_calls().await {
        let query = serde_json::to_value(&call.query).unwrap();
        let filters = query["filters"].as_array().cloned().unwrap_or_default();
        let time_dimensions = query["timeDimensions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for filter in filters {
            if filter["member"] == member {
                for value in filter["values"].as_array().into_iter().flatten() {
                    values.push(value.as_str().unwrap().to_string());
                }
            }
        }
        for time_dimension in time_dimensions {
            if time_dimension["dimension"] == member {
                for value in time_dimension["dateRange"].as_array().into_iter().flatten() {
                    values.push(value.as_str().unwrap().to_string());
                }
            }
        }
    }
    values
}

/// At Parse a parameter has no value yet, but the SQL generator gets the filter with a
/// value in its place, and the planners refuse a time filter value that is not a date.
#[tokio::test]
async fn test_parse_gives_a_date_for_a_parameter_on_a_time_member() {
    if !crate::compile::rewrite::rewriter::Rewriter::sql_push_down_enabled() {
        return;
    }

    let source = "SELECT taxful_total_price, customer_gender, order_date \
        FROM KibanaSampleDataEcommerce";

    for condition in [
        "order_date >= $1",
        "order_date > $1",
        "order_date <= $1",
        "order_date < $1",
        "order_date = $1",
        "order_date >= $1 AND order_date < $2",
        "order_date BETWEEN $1 AND $2",
        "order_date >= $1 AND customer_gender = $2",
        "order_date IS NOT NULL AND order_date >= $1",
        "order_date IN ($1, $2)",
        "DATE_TRUNC('day', order_date) >= $1",
    ] {
        let (context, client) = serve().await;
        let query =
            format!("SELECT SUM(taxful_total_price) AS s FROM ({source}) AS q WHERE {condition}");

        client
            .prepare(&query)
            .await
            .unwrap_or_else(|err| panic!("{} must parse: {}", condition, err));

        for value in filter_values(&context, "KibanaSampleDataEcommerce.order_date").await {
            assert!(
                chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
                    || chrono::DateTime::parse_from_rfc3339(&value).is_ok(),
                "{}: {} is not a date",
                condition,
                value
            );
        }
    }
}

/// The week number form that BI tools send, with the filter inside of a grouped subquery.
#[tokio::test]
async fn test_parse_gives_a_date_for_a_parameter_in_a_grouped_subquery() {
    if !crate::compile::rewrite::rewriter::Rewriter::sql_push_down_enabled() {
        return;
    }

    let (context, client) = serve().await;
    client
        .prepare(
            "SELECT w, c FROM (\
            SELECT CAST(FLOOR(DATEDIFF('day', TIMESTAMP '2001-01-07 00:00:00', order_date) / 7.0) AS bigint) AS w, \
            MEASURE(count) AS c FROM KibanaSampleDataEcommerce WHERE order_date >= $1 GROUP BY 1\
            ) AS q ORDER BY 1",
        )
        .await
        .expect("the query must parse");

    for value in filter_values(&context, "KibanaSampleDataEcommerce.order_date").await {
        assert!(
            chrono::DateTime::parse_from_rfc3339(&value).is_ok(),
            "{} is not a date",
            value
        );
    }
}

/// Arithmetic on a measure and an ORDER BY push the whole query down, as in #12459.
#[tokio::test]
async fn test_parse_gives_a_date_for_a_parameter_with_measure_arithmetic() {
    if !crate::compile::rewrite::rewriter::Rewriter::sql_push_down_enabled() {
        return;
    }

    for suffix in ["ORDER BY 1, 2", "ORDER BY 1, 2 LIMIT 10"] {
        let (context, client) = serve().await;
        client
            .prepare(&format!(
                "SELECT DATE_TRUNC('month', order_date) AS m, customer_gender, \
                SUM(sumPrice) * 0.5 AS x FROM KibanaSampleDataEcommerce \
                WHERE order_date >= $1 GROUP BY 1, 2 {suffix}"
            ))
            .await
            .unwrap_or_else(|err| panic!("{} must parse: {}", suffix, err));

        for value in filter_values(&context, "KibanaSampleDataEcommerce.order_date").await {
            assert!(
                chrono::DateTime::parse_from_rfc3339(&value).is_ok(),
                "{} is not a date",
                value
            );
        }
    }
}

/// The value at Parse must not stay: Bind plans the query again with the value of the client.
#[tokio::test]
async fn test_bind_replaces_the_date_of_a_parameter() {
    if !crate::compile::rewrite::rewriter::Rewriter::sql_push_down_enabled() {
        return;
    }

    let (context, client) = serve().await;
    let statement = client
        .prepare(
            "SELECT SUM(taxful_total_price) AS s \
            FROM (SELECT taxful_total_price, order_date FROM KibanaSampleDataEcommerce) AS q \
            WHERE order_date >= $1",
        )
        .await
        .expect("the query must parse");

    // The transport has no rows for this query, and only its SQL request matters here
    let _ = client
        .query(&statement, &[&"2025-05-10T00:00:00.000"])
        .await;

    let values = filter_values(&context, "KibanaSampleDataEcommerce.order_date").await;
    assert_eq!(
        values.last().map(String::as_str),
        Some("2025-05-10T00:00:00.000Z")
    );
}

/// A parameter on a member that is not a time stays a plain value in each form.
#[tokio::test]
async fn test_parse_plans_a_parameter_on_other_members() {
    for query in [
        "SELECT customer_gender, MEASURE(count) AS c FROM KibanaSampleDataEcommerce \
            WHERE customer_gender = $1 GROUP BY 1",
        "SELECT customer_gender, MEASURE(count) AS c FROM KibanaSampleDataEcommerce \
            WHERE customer_gender IN ($1, $2) GROUP BY 1",
        "SELECT customer_gender, MEASURE(count) AS c FROM KibanaSampleDataEcommerce \
            WHERE customer_gender LIKE $1 GROUP BY 1",
        "SELECT customer_gender, MEASURE(count) AS c FROM KibanaSampleDataEcommerce \
            WHERE taxful_total_price > $1 GROUP BY 1",
        "SELECT customer_gender, MEASURE(count) AS c FROM KibanaSampleDataEcommerce \
            GROUP BY 1 LIMIT $1",
    ] {
        let (_context, client) = serve().await;
        client
            .prepare(query)
            .await
            .unwrap_or_else(|err| panic!("{} must parse: {}", query, err));
    }
}
