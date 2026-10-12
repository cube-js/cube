use bytes::{BufMut, BytesMut};
use cubeclient::models::{V1LoadRequestQuery, V1LoadRequestQueryFilterItem};
use pretty_assertions::assert_eq;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

use crate::compile::{
    rewrite::rewriter::Rewriter,
    test::{
        convert_select_to_query_plan, execute_query, init_testing_logger,
        utils::LogicalPlanTestUtils, TestContext,
    },
    DatabaseProtocol,
};
use crate::{sql::postgres::shim::AsyncPostgresShim, telemetry::SessionLogger};

/// https://github.com/cube-js/cube/issues/10581
/// Metabase v1.59+ appends `ESCAPE '\'` to every LIKE it generates; that should be
/// rewritten exactly like the same LIKE without ESCAPE (backslash is the default).
#[tokio::test]
async fn test_issue_10581_metabase_like_escape_backslash() {
    init_testing_logger();

    let logical_plan = convert_select_to_query_plan(
        r#"
        SELECT "public"."KibanaSampleDataEcommerce"."count" AS "count"
        FROM "public"."KibanaSampleDataEcommerce"
        WHERE lower("public"."KibanaSampleDataEcommerce"."customer_gender") LIKE '%female%' ESCAPE '\'
        LIMIT 10
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .as_logical_plan();

    assert_eq!(
        logical_plan.find_cube_scan().request,
        V1LoadRequestQuery {
            measures: Some(vec!["KibanaSampleDataEcommerce.count".to_string()]),
            dimensions: Some(vec![]),
            segments: Some(vec![]),
            order: Some(vec![]),
            limit: Some(10),
            ungrouped: Some(true),
            filters: Some(vec![V1LoadRequestQueryFilterItem {
                member: Some("KibanaSampleDataEcommerce.customer_gender".to_string()),
                operator: Some("contains".to_string()),
                values: Some(vec!["female".to_string()]),
                or: None,
                and: None,
            }]),
            ..Default::default()
        }
    );
}

/// https://github.com/cube-js/cube/issues/10307
/// Postgrex bootstraps types from `pg_type` and expects `typsend` / `typoutput` to be
/// non-NULL (an OID / regproc, or 0), like `typreceive` already is.
#[tokio::test]
async fn test_issue_10307_pg_type_typsend_typoutput_not_null() {
    init_testing_logger();

    let result = execute_query(
        r#"
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN t.typsend IS NULL THEN 1 ELSE 0 END) AS typsend_nulls,
            SUM(CASE WHEN t.typoutput IS NULL THEN 1 ELSE 0 END) AS typoutput_nulls,
            SUM(CASE WHEN t.typreceive IS NULL THEN 1 ELSE 0 END) AS typreceive_nulls
        FROM pg_type AS t
        "#
        .to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await
    .unwrap();

    // Every row must have non-NULL typsend/typoutput/typreceive
    let data_line = result
        .lines()
        .find(|l| l.starts_with("| ") && !l.contains("total"))
        .expect("result row")
        .to_string();
    let cells: Vec<&str> = data_line
        .split('|')
        .map(|c| c.trim())
        .filter(|c| !c.is_empty())
        .collect();
    assert_eq!(
        &cells[1..],
        &["0", "0", "0"],
        "NULL counts for typsend/typoutput/typreceive, full result:\n{}",
        result
    );
}

/// https://github.com/cube-js/cube/issues/10320
/// Felt introspection query: a CTE built from a UNION ALL of other CTEs, referenced by
/// its name (no alias) in a join condition.
#[tokio::test]
async fn test_issue_10320_felt_introspection_cte_union_join() {
    init_testing_logger();

    let excluded = "('information_schema', 'ogr_system_tables', '_timescaledb_catalog', '_timescaledb_functions', '_timescaledb_internal', '_timescaledb_cache', '_timescaledb_config', 'timescaledb_experimental', 'timescaledb_information', '_timescaledb_debug', 'auth', 'extensions', 'graphql', 'graphql_public', 'realtime', 'storage', 'vault', 'cron')";
    let query = format!(
        "WITH tables AS (SELECT table_name AS relation_name, table_schema AS relation_schema, 'table' AS relation_type FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = 'public' AND table_schema NOT IN {excluded} AND table_schema NOT ILIKE 'pg%%' AND table_name NOT IN ('spatial_ref_sys') AND table_name NOT ILIKE 'pg%%'), \
         views AS (SELECT table_name AS relation_name, table_schema AS relation_schema, 'view' AS relation_type FROM information_schema.views WHERE table_schema NOT IN {excluded} AND table_schema = 'public' AND table_schema NOT ILIKE 'pg%%' AND table_name NOT IN ('geometry_columns', 'geography_columns', 'raster_columns', 'raster_overviews') AND table_name NOT ILIKE 'pg%%'), \
         materialized_views AS (SELECT matviewname AS relation_name, schemaname AS relation_schema, 'materialized_view' AS relation_type FROM pg_matviews WHERE schemaname NOT IN {excluded} AND schemaname = 'public' AND schemaname NOT ILIKE 'pg%%' AND matviewname NOT ILIKE 'pg%%'), \
         all_tables AS (SELECT * FROM tables UNION ALL SELECT * FROM views UNION ALL SELECT * FROM materialized_views) \
         SELECT all_tables.* FROM all_tables JOIN pg_namespace AS n ON n.nspname = all_tables.relation_schema JOIN pg_class AS c ON c.relname = all_tables.relation_name AND c.relnamespace = n.oid AND c.relkind IN ('r', 'v', 'm')"
    );

    let result = execute_query(query, DatabaseProtocol::PostgreSQL)
        .await
        .unwrap();
    assert!(result.contains("KibanaSampleDataEcommerce"), "{}", result);
}

/// https://github.com/cube-js/cube/issues/10712
/// `TRIM(dim)` must be pushed down as a narrow member expression, just like
/// `LTRIM(dim)` / `RTRIM(dim)` / `LOWER(dim)` are. Today there is no `functions/TRIM`
/// SQL template (BaseQuery only has BTRIM/LTRIM/RTRIM), so the projection can't be
/// pushed into the wrapper and the query is not planned as a narrow Cube query.
#[tokio::test]
async fn test_issue_10712_trim_dimension_member_expression() {
    if !Rewriter::sql_push_down_enabled() {
        return;
    }
    init_testing_logger();

    // Same function templates the schema compiler's BaseQuery exposes
    let templates = vec![
        (
            "functions/LTRIM".to_string(),
            "LTRIM({{ args_concat }})".to_string(),
        ),
        (
            "functions/RTRIM".to_string(),
            "RTRIM({{ args_concat }})".to_string(),
        ),
        (
            "functions/BTRIM".to_string(),
            "BTRIM({{ args_concat }})".to_string(),
        ),
    ];

    for func in ["LTRIM", "TRIM"] {
        let query = format!(
            r#"
            SELECT
                {func}(customer_gender) AS g,
                MEASURE(count) AS c,
                MEASURE(sumPrice) AS s
            FROM KibanaSampleDataEcommerce
            GROUP BY 1
            ORDER BY c DESC
            LIMIT 10
            "#
        );

        let plan =
            TestContext::with_custom_templates(DatabaseProtocol::PostgreSQL, templates.clone())
                .await
                .convert_sql_to_cube_query(&query)
                .await
                .unwrap_or_else(|err| panic!("{}(dim) must be planned, got: {:?}", func, err))
                .as_logical_plan();

        let request = plan.find_cube_scan_wrapped_sql().request;
        assert_eq!(
            request.measures.as_ref().map(|m| m.len()),
            Some(2),
            "{func}: {request:?}"
        );
        let dimensions = request.dimensions.clone().unwrap_or_default();
        assert_eq!(dimensions.len(), 1, "{func}: {request:?}");
        assert!(
            dimensions[0].contains("TRIM(${KibanaSampleDataEcommerce.customer_gender})"),
            "{func}: {}",
            dimensions[0]
        );
        assert_eq!(request.ungrouped, None, "{func}: {request:?}");
    }
}

/// https://github.com/cube-js/cube/issues/10962
/// When Parse fails in the extended query protocol, the backend must discard every
/// message until Sync. Bind is still processed today and a BindComplete is sent to
/// the client before the ErrorResponse, which desyncs npgsql.
#[tokio::test]
async fn test_issue_10962_bind_after_failed_parse_is_discarded() {
    async fn read_message(socket: &mut TcpStream) -> (u8, Vec<u8>) {
        let read = async {
            let tag = socket.read_u8().await.expect("must read a message tag");
            let length = socket.read_u32().await.expect("must read a message length") as usize;
            let mut body = vec![0; length - 4];
            socket
                .read_exact(&mut body)
                .await
                .expect("must read a message body");

            (tag, body)
        };

        tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .expect("server must answer")
    }

    fn message(tag: u8, payload: &[u8]) -> BytesMut {
        let mut message = BytesMut::new();
        message.put_u8(tag);
        message.put_u32(4 + payload.len() as u32);
        message.extend_from_slice(payload);
        message
    }

    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    let session_manager = context.session.session_manager.clone();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("must bind a port");
    let port = listener.local_addr().unwrap().port();

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

        let _ = AsyncPostgresShim::run_on(
            CancellationToken::new(),
            CancellationToken::new(),
            socket,
            session,
            logger,
        )
        .await;
    });

    let mut socket = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("must connect");

    let parameters: &[u8] = b"user\0test\0database\0db\0\0";
    let mut startup = BytesMut::new();
    startup.put_u32(4 + 4 + parameters.len() as u32);
    // Protocol version 3.0
    startup.put_u32(196608);
    startup.extend_from_slice(parameters);
    socket.write_all(&startup).await.expect("must write");

    let (tag, _) = read_message(&mut socket).await;
    assert_eq!(tag, b'R', "server must ask for authentication");
    socket
        .write_all(&message(b'p', b"test\0"))
        .await
        .expect("must write");
    while read_message(&mut socket).await.0 != b'Z' {}

    // Parse (unnamed statement) with a syntax error
    let mut parse = BytesMut::new();
    parse.extend_from_slice(b"\0SELEC 1\0");
    parse.put_i16(0);
    // Bind unnamed portal to unnamed statement, no params, no result formats
    let mut bind = BytesMut::new();
    bind.extend_from_slice(b"\0\0");
    bind.put_i16(0);
    bind.put_i16(0);
    bind.put_i16(0);
    // Execute unnamed portal, no row limit
    let mut execute = BytesMut::new();
    execute.extend_from_slice(b"\0");
    execute.put_i32(0);

    let mut batch = BytesMut::new();
    batch.extend_from_slice(&message(b'P', &parse));
    batch.extend_from_slice(&message(b'B', &bind));
    batch.extend_from_slice(&message(b'E', &execute));
    batch.extend_from_slice(&message(b'S', b""));
    socket.write_all(&batch).await.expect("must write");

    let mut tags = vec![];
    loop {
        let (tag, _) = read_message(&mut socket).await;
        tags.push(tag as char);
        if tag == b'Z' {
            break;
        }
    }

    // Only ErrorResponse then ReadyForQuery; no BindComplete ('2')
    assert_eq!(tags, vec!['E', 'Z']);
}
