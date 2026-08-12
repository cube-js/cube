use crate::{
    compile::{test::TestContext, DatabaseProtocol},
    sql::postgres::shim::AsyncPostgresShim,
    telemetry::SessionLogger,
    CubeError,
};
use bytes::{BufMut, BytesMut};
use futures::SinkExt;
use pretty_assertions::assert_eq;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tokio_util::sync::CancellationToken;

/// Serve connections of a single session, so that temporary tables created by one
/// connection are visible to the next one. Returns the port to connect to.
async fn serve_session() -> u16 {
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    let session_manager = context.session.session_manager.clone();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("must bind a port");
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
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

            tokio::spawn(async move {
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
        }
    });

    port
}

async fn connect(port: u16) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test password=test", port),
        NoTls,
    )
    .await
    .expect("must connect");

    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
}

async fn query_err(client: &Client, query: &str) -> String {
    match client.simple_query(query).await {
        Ok(_) => panic!("expected an error for: {}", query),
        Err(err) => err.to_string(),
    }
}

/// Rows of a query as pipe-separated strings, NULL shown as an empty value.
async fn query_rows(client: &Client, query: &str) -> Result<Vec<String>, CubeError> {
    let messages = client
        .simple_query(query)
        .await
        .map_err(|err| CubeError::internal(err.to_string()))?;

    Ok(messages
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
        .collect())
}

#[tokio::test]
async fn test_copy_from_stdin_text_format() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    client
        .simple_query("CREATE TEMPORARY TABLE t (n int, s text, b boolean)")
        .await
        .expect("temporary table must be created");

    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY t FROM STDIN")
            .await
            .expect("COPY must start"),
    );

    // Two CopyData messages, with a row split across them
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(b"1\tone\tt\n2\tt"))
        .await
        .expect("must send data");
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(b"wo\tf\n3\t\\N\tt\n"))
        .await
        .expect("must send data");

    let rows = writer.as_mut().finish().await.expect("COPY must finish");
    assert_eq!(rows, 3);

    assert_eq!(
        query_rows(&client, "SELECT n, s, b FROM t ORDER BY n").await?,
        vec!["1|one|t", "2|two|f", "3||t"]
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_from_stdin_csv_format() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    client
        .simple_query("CREATE TEMPORARY TABLE t (n int, s text, b boolean)")
        .await
        .expect("temporary table must be created");

    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY t (n, s) FROM STDIN WITH (FORMAT csv, HEADER)")
            .await
            .expect("COPY must start"),
    );

    writer
        .as_mut()
        .send(bytes::Bytes::from_static(
            b"n,s\n1,\"a,b\"\n2,\n3,\"quote\"\"inside\"\n",
        ))
        .await
        .expect("must send data");

    let rows = writer.as_mut().finish().await.expect("COPY must finish");
    assert_eq!(rows, 3);

    // The column not listed in the COPY statement stays NULL
    assert_eq!(
        query_rows(&client, "SELECT n, s, b FROM t ORDER BY n").await?,
        vec!["1|a,b|", "2||", "3|quote\"inside|"]
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_from_stdin_appends() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    client
        .simple_query("CREATE TEMPORARY TABLE t (n int)")
        .await
        .expect("temporary table must be created");

    for value in ["1", "2"] {
        let mut writer = Box::pin(
            client
                .copy_in::<_, bytes::Bytes>("COPY t FROM STDIN")
                .await
                .expect("COPY must start"),
        );
        writer
            .as_mut()
            .send(bytes::Bytes::from(format!("{}\n", value)))
            .await
            .expect("must send data");

        assert_eq!(writer.as_mut().finish().await.expect("COPY must finish"), 1);
    }

    assert_eq!(
        query_rows(&client, "SELECT n FROM t ORDER BY n").await?,
        vec!["1", "2"]
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_from_stdin_errors() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    let err = query_err(&client, "COPY unknown_table FROM STDIN").await;
    assert!(
        err.contains("COPY FROM STDIN is only supported for temporary tables"),
        "unexpected error: {}",
        err
    );

    client
        .simple_query("CREATE TEMPORARY TABLE t (n int)")
        .await
        .expect("temporary table must be created");

    let err = query_err(&client, "COPY t (missing) FROM STDIN").await;
    assert!(
        err.contains(r#"column "missing" of relation "t" does not exist"#),
        "unexpected error: {}",
        err
    );

    // A value which does not match the column type fails the copy
    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY t FROM STDIN")
            .await
            .expect("COPY must start"),
    );
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(b"not-a-number\n"))
        .await
        .expect("must send data");

    let err = writer
        .as_mut()
        .finish()
        .await
        .expect_err("must fail")
        .to_string();
    assert!(
        err.contains("invalid input syntax for type integer"),
        "unexpected error: {}",
        err
    );

    // The failed copy left the table empty and the session usable
    assert_eq!(
        query_rows(&client, "SELECT n FROM t").await?,
        Vec::<String>::new()
    );

    Ok(())
}

/// Run a COPY with the simple query protocol, the one `psql` uses for `\copy`.
/// Returns the tags of the received messages and the command completion tag.
async fn copy_in_simple_query(port: u16, statements: &[&str], data: &str) -> (Vec<u8>, String) {
    async fn read_message(socket: &mut TcpStream) -> (u8, Vec<u8>) {
        let tag = socket.read_u8().await.expect("must read a message tag");
        let length = socket.read_u32().await.expect("must read a message length") as usize;
        let mut body = vec![0; length - 4];
        socket
            .read_exact(&mut body)
            .await
            .expect("must read a message body");

        (tag, body)
    }

    async fn simple_query(socket: &mut TcpStream, query: &str) {
        let mut message = BytesMut::new();
        message.put_u8(b'Q');
        message.put_u32(4 + query.len() as u32 + 1);
        message.extend_from_slice(query.as_bytes());
        message.put_u8(0);
        socket.write_all(&message).await.expect("must write");
    }

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

    let mut password = BytesMut::new();
    password.put_u8(b'p');
    password.put_u32(4 + 5);
    password.extend_from_slice(b"test\0");
    socket.write_all(&password).await.expect("must write");

    // Skip the authentication result, parameter statuses and the key data
    while read_message(&mut socket).await.0 != b'Z' {}

    for statement in statements {
        simple_query(&mut socket, statement).await;
        while read_message(&mut socket).await.0 != b'Z' {}
    }

    simple_query(&mut socket, "COPY t FROM STDIN").await;

    let mut tags = vec![];
    let (tag, _) = read_message(&mut socket).await;
    tags.push(tag);
    assert_eq!(tag, b'G', "server must be ready to receive the data");

    let mut copy_data = BytesMut::new();
    copy_data.put_u8(b'd');
    copy_data.put_u32(4 + data.len() as u32);
    copy_data.extend_from_slice(data.as_bytes());
    socket.write_all(&copy_data).await.expect("must write");

    let mut copy_done = BytesMut::new();
    copy_done.put_u8(b'c');
    copy_done.put_u32(4);
    socket.write_all(&copy_done).await.expect("must write");

    let mut completion = String::new();
    loop {
        let (tag, body) = read_message(&mut socket).await;
        tags.push(tag);

        match tag {
            b'C' => completion = String::from_utf8_lossy(&body[..body.len() - 1]).to_string(),
            b'Z' => break,
            _ => (),
        }
    }

    (tags, completion)
}

#[tokio::test]
async fn test_copy_from_stdin_simple_query() {
    let port = serve_session().await;

    let (tags, completion) = copy_in_simple_query(
        port,
        &["CREATE TEMPORARY TABLE t (n int, s text)"],
        "1\tone\n2\ttwo\n\\.\n",
    )
    .await;

    // CopyInResponse, CommandComplete, ReadyForQuery
    assert_eq!(tags, vec![b'G', b'C', b'Z']);
    assert_eq!(completion, "COPY 2");
}

#[tokio::test]
async fn test_copy_from_stdin_column_types() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    client
        .simple_query(
            "CREATE TEMPORARY TABLE t (
                i smallint,
                n numeric(10, 2),
                f double precision,
                d date,
                ts timestamp,
                s varchar(10) NOT NULL
            )",
        )
        .await
        .expect("temporary table must be created");

    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY t FROM STDIN")
            .await
            .expect("COPY must start"),
    );
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(
            b"7\t12.34\t1.5\t2024-03-01\t2024-03-01 10:20:30\tvalue\n",
        ))
        .await
        .expect("must send data");

    assert_eq!(writer.as_mut().finish().await.expect("COPY must finish"), 1);

    assert_eq!(
        query_rows(&client, "SELECT i, n, f, d, ts, s FROM t").await?,
        vec!["7|12.34|1.5|2024-03-01|2024-03-01 10:20:30.000000|value"]
    );

    // The table can be dropped like any other temporary table
    client
        .simple_query("DROP TABLE t")
        .await
        .expect("temporary table must be dropped");

    let err = query_err(&client, "SELECT i FROM t").await;
    assert!(
        err.contains("Table or CTE with name 't' not found"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_create_temporary_table_errors() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    client
        .simple_query("CREATE TEMPORARY TABLE t (n int)")
        .await
        .expect("temporary table must be created");

    let err = query_err(&client, "CREATE TEMPORARY TABLE t (n int)").await;
    assert!(
        err.contains(r#"relation "t" already exists"#),
        "unexpected error: {}",
        err
    );

    let err = query_err(&client, "CREATE TEMPORARY TABLE other (b bytea)").await;
    assert!(
        err.contains("Unsupported column type for a temporary table: BYTEA"),
        "unexpected error: {}",
        err
    );

    let err = query_err(&client, "CREATE TEMPORARY TABLE other (n int DEFAULT 1)").await;
    assert!(
        err.contains("Unsupported column option for a temporary table: DEFAULT 1"),
        "unexpected error: {}",
        err
    );

    // A non-temporary table is still not something Cube can create
    let err = query_err(&client, "CREATE TABLE other (n int)").await;
    assert!(
        err.contains("Unsupported query type"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_from_stdin_into_table_created_by_query() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    client
        .simple_query("CREATE TEMPORARY TABLE t AS SELECT 1 AS n, 'one' AS s")
        .await
        .expect("temporary table must be created");

    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY t FROM STDIN")
            .await
            .expect("COPY must start"),
    );
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(b"2\ttwo\n"))
        .await
        .expect("must send data");

    assert_eq!(writer.as_mut().finish().await.expect("COPY must finish"), 1);

    assert_eq!(
        query_rows(&client, "SELECT n, s FROM t ORDER BY n").await?,
        vec!["1|one", "2|two"]
    );

    Ok(())
}

/// Drive a COPY FROM STDIN by hand over the simple query protocol, so that the exact
/// message exchange can be checked. After the data chunks, `trailer` messages are sent
/// as given, which is how a copy is ended, aborted or interrupted. Returns the tags of
/// the received messages and, for a completion its tag, for an error its fields.
async fn copy_in_exchange(
    port: u16,
    setup: &[&str],
    copy: &str,
    data: &[&str],
    trailer: &[(u8, &str)],
) -> (Vec<u8>, String) {
    /// Reading has a timeout so that a server which never answers fails the test
    /// instead of hanging it.
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

    async fn send(socket: &mut TcpStream, tag: u8, payload: &[u8]) {
        let mut message = BytesMut::new();
        message.put_u8(tag);
        message.put_u32(4 + payload.len() as u32);
        message.extend_from_slice(payload);
        socket.write_all(&message).await.expect("must write");
    }

    async fn query(socket: &mut TcpStream, sql: &str) {
        let mut payload = BytesMut::new();
        payload.extend_from_slice(sql.as_bytes());
        payload.put_u8(0);
        send(socket, b'Q', &payload).await;
    }

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
    send(&mut socket, b'p', b"test\0").await;
    while read_message(&mut socket).await.0 != b'Z' {}

    for statement in setup {
        query(&mut socket, statement).await;
        while read_message(&mut socket).await.0 != b'Z' {}
    }

    query(&mut socket, copy).await;

    let mut tags = vec![];
    let (tag, _) = read_message(&mut socket).await;
    tags.push(tag);
    assert_eq!(tag, b'G', "server must be ready to receive the data");

    for chunk in data {
        send(&mut socket, b'd', chunk.as_bytes()).await;
    }

    for (tag, payload) in trailer {
        let mut body = BytesMut::new();
        body.extend_from_slice(payload.as_bytes());
        // A Query or a CopyFail carries a null-terminated string
        if *tag == b'f' || *tag == b'Q' {
            body.put_u8(0);
        }
        send(&mut socket, *tag, &body).await;
    }

    let mut result = String::new();
    loop {
        let (tag, body) = read_message(&mut socket).await;
        tags.push(tag);

        match tag {
            // CommandComplete: the tag of the command
            b'C' => result = String::from_utf8_lossy(&body[..body.len() - 1]).to_string(),
            // ErrorResponse: the code, the message and the context
            b'E' => {
                result = String::from_utf8_lossy(&body)
                    .split('\0')
                    .filter(|field| {
                        field.starts_with('C') || field.starts_with('M') || field.starts_with('W')
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
            }
            b'Z' => break,
            _ => (),
        }
    }

    (tags, result)
}

#[tokio::test]
async fn test_copy_in_error_stops_the_copy_at_once() {
    let port = serve_session().await;

    // PostgreSQL reports the error as soon as it sees the bad row and drops the
    // CopyData and CopyDone messages the client keeps sending afterwards
    let (tags, error) = copy_in_exchange(
        port,
        &["CREATE TEMPORARY TABLE t (n int)"],
        "COPY t FROM STDIN",
        &["1\noops\n", "2\n"],
        &[(b'c', "")],
    )
    .await;

    assert_eq!(tags, vec![b'G', b'E', b'Z']);
    assert_eq!(
        error,
        "C22P02 Minvalid input syntax for type integer: \"oops\" WCOPY t, line 2, column n: \"oops\""
    );
}

#[tokio::test]
async fn test_copy_in_client_abort() {
    let port = serve_session().await;

    let (tags, error) = copy_in_exchange(
        port,
        &["CREATE TEMPORARY TABLE t (n int)"],
        "COPY t FROM STDIN",
        &["1\n"],
        &[(b'f', "aborted by the client")],
    )
    .await;

    assert_eq!(tags, vec![b'G', b'E', b'Z']);
    assert_eq!(
        error,
        "C57014 MCOPY from stdin failed: aborted by the client"
    );
}

#[tokio::test]
async fn test_copy_in_unexpected_message() {
    let port = serve_session().await;

    // A Query message in the middle of a copy is a protocol violation
    let (tags, error) = copy_in_exchange(
        port,
        &["CREATE TEMPORARY TABLE t (n int)"],
        "COPY t FROM STDIN",
        &["1\n"],
        &[(b'Q', "SELECT 1")],
    )
    .await;

    assert_eq!(tags, vec![b'G', b'E', b'Z']);
    assert_eq!(
        error,
        "C08P01 Munexpected message type 0x51 during COPY from stdin"
    );
}

#[tokio::test]
async fn test_copy_in_end_of_data_marker_completes_the_copy() {
    let port = serve_session().await;

    // The marker ends the copy, so the trailing CopyDone is dropped
    let (tags, completion) = copy_in_exchange(
        port,
        &["CREATE TEMPORARY TABLE t (n int)"],
        "COPY t FROM STDIN",
        &["1\n2\n\\.\n"],
        &[(b'c', "")],
    )
    .await;

    assert_eq!(tags, vec![b'G', b'C', b'Z']);
    assert_eq!(completion, "COPY 2");
}

#[tokio::test]
async fn test_copy_in_ignores_flush_and_sync() {
    let port = serve_session().await;

    // Flush and Sync are ignored while the data is being read, so the copy is still
    // ended by the CopyDone which follows them
    let (tags, completion) = copy_in_exchange(
        port,
        &["CREATE TEMPORARY TABLE t (n int)"],
        "COPY t FROM STDIN",
        &["1\n"],
        &[(b'H', ""), (b'S', ""), (b'c', "")],
    )
    .await;

    assert_eq!(tags, vec![b'G', b'C', b'Z']);
    assert_eq!(completion, "COPY 1");
}

#[tokio::test]
async fn test_create_temporary_table_over_the_extended_protocol() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    // The extended protocol plans a statement at Parse and again at Bind, so a table
    // may only be created when the statement is executed
    client
        .execute("CREATE TEMPORARY TABLE t (n int, s text)", &[])
        .await
        .expect("temporary table must be created");

    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY t FROM STDIN")
            .await
            .expect("COPY must start"),
    );
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(b"1\tone\n"))
        .await
        .expect("must send data");
    assert_eq!(writer.as_mut().finish().await.expect("COPY must finish"), 1);

    assert_eq!(
        query_rows(&client, "SELECT n, s FROM t").await?,
        vec!["1|one"]
    );

    // Planning a statement which is never executed leaves nothing behind
    client
        .prepare("CREATE TEMPORARY TABLE planned_only (n int)")
        .await
        .expect("statement must be prepared");

    let err = query_err(&client, "SELECT n FROM planned_only").await;
    assert!(
        err.contains("'planned_only' not found") || err.contains("planned_only"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_from_stdin_folds_option_column_names() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    client
        .simple_query("CREATE TEMPORARY TABLE t (n int, s text)")
        .await
        .expect("temporary table must be created");

    // An unquoted column name of an option is folded like any other identifier
    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>(
                "COPY t FROM STDIN WITH (FORMAT csv, NULL 'nil', FORCE_NOT_NULL (S))",
            )
            .await
            .expect("COPY must start"),
    );
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(b"1,nil\n"))
        .await
        .expect("must send data");
    assert_eq!(writer.as_mut().finish().await.expect("COPY must finish"), 1);

    assert_eq!(
        query_rows(&client, "SELECT n, s FROM t").await?,
        vec!["1|nil"]
    );

    Ok(())
}

#[tokio::test]
async fn test_create_temporary_table_rejects_unsupported_types() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    // A time zone changes what the values mean, so the column is refused rather than
    // quietly stored without one
    let err = query_err(&client, "CREATE TEMPORARY TABLE t (ts timestamptz)").await;
    assert!(
        err.contains("Unsupported column type for a temporary table: TIMESTAMPTZ"),
        "unexpected error: {}",
        err
    );

    let err = query_err(&client, "CREATE TEMPORARY TABLE t (d numeric(50, 2))").await;
    assert!(
        err.contains("NUMERIC precision 50 must be between 1 and 38"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_create_temporary_table_if_not_exists() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    for statement in [
        "CREATE TEMPORARY TABLE t (n int)",
        // An existing table makes the statement a no-op instead of an error, and the
        // table it found keeps the shape it had
        "CREATE TEMPORARY TABLE IF NOT EXISTS t (n int, s text)",
        "CREATE TEMPORARY TABLE IF NOT EXISTS t AS SELECT 1 AS n, 2 AS m",
    ] {
        client
            .simple_query(statement)
            .await
            .unwrap_or_else(|err| panic!("{} must succeed: {}", statement, err));
    }

    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY t FROM STDIN")
            .await
            .expect("COPY must start"),
    );
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(b"1\n"))
        .await
        .expect("must send data");
    assert_eq!(writer.as_mut().finish().await.expect("COPY must finish"), 1);

    assert_eq!(query_rows(&client, "SELECT n FROM t").await?, vec!["1"]);

    // Without IF NOT EXISTS the name is still taken
    let err = query_err(&client, "CREATE TEMPORARY TABLE t (n int)").await;
    assert!(
        err.contains(r#"relation "t" already exists"#),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_from_stdin_option_columns_must_be_copied() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    client
        .simple_query("CREATE TEMPORARY TABLE t (n int, s text)")
        .await
        .expect("temporary table must be created");

    let err = query_err(
        &client,
        "COPY t (n) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (s))",
    )
    .await;
    assert!(
        err.contains(r#"FORCE_NOT_NULL column "s" not referenced by COPY"#),
        "unexpected error: {}",
        err
    );

    let err = query_err(
        &client,
        "COPY t (n) FROM STDIN WITH (FORMAT csv, FORCE_NULL (s))",
    )
    .await;
    assert!(
        err.contains(r#"FORCE_NULL column "s" not referenced by COPY"#),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_from_stdin_encoding_aliases() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    client
        .simple_query("CREATE TEMPORARY TABLE t (n int)")
        .await
        .expect("temporary table must be created");

    // UTF8 goes by a few names, and only UTF8 is supported
    for encoding in ["UTF8", "utf-8", "unicode"] {
        let mut writer = Box::pin(
            client
                .copy_in::<_, bytes::Bytes>(&format!(
                    "COPY t FROM STDIN WITH (ENCODING '{}')",
                    encoding
                ))
                .await
                .unwrap_or_else(|err| panic!("{} must be accepted: {}", encoding, err)),
        );
        writer
            .as_mut()
            .send(bytes::Bytes::from_static(b"1\n"))
            .await
            .expect("must send data");
        writer.as_mut().finish().await.expect("COPY must finish");
    }

    let err = query_err(&client, "COPY t FROM STDIN WITH (ENCODING 'LATIN1')").await;
    assert!(
        err.contains("COPY ENCODING is only supported for UTF8"),
        "unexpected error: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_from_stdin_character_types() -> Result<(), CubeError> {
    let client = connect(serve_session().await).await;

    // A fixed width character type is blank padded and compares without its trailing
    // blanks, which a text column cannot do, so the type is refused outright
    for statement in [
        "CREATE TEMPORARY TABLE fixed (c char(3))",
        "CREATE TEMPORARY TABLE fixed (c character(3))",
        "CREATE TEMPORARY TABLE fixed (c char)",
    ] {
        let err = query_err(&client, statement).await;
        assert!(
            err.contains("Unsupported column type for a temporary table")
                && err.contains("use VARCHAR or TEXT"),
            "unexpected error for {}: {}",
            statement,
            err
        );
    }

    client
        .simple_query("CREATE TEMPORARY TABLE t (v varchar(3), b boolean)")
        .await
        .expect("temporary table must be created");

    // A value wider than the column is reported against the type it was declared as
    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY t (v) FROM STDIN")
            .await
            .expect("COPY must start"),
    );
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(b"toolong\n"))
        .await
        .expect("must send data");

    let err = writer
        .as_mut()
        .finish()
        .await
        .expect_err("must fail")
        .to_string();
    assert!(
        err.contains("value too long for type character varying(3)"),
        "unexpected error: {}",
        err
    );

    // An empty field is neither a NULL nor a spelling of true
    let mut writer = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY t (b) FROM STDIN")
            .await
            .expect("COPY must start"),
    );
    writer
        .as_mut()
        .send(bytes::Bytes::from_static(b"\n"))
        .await
        .expect("must send data");

    let err = writer
        .as_mut()
        .finish()
        .await
        .expect_err("must fail")
        .to_string();
    assert!(
        err.contains(r#"invalid input syntax for type boolean: """#),
        "unexpected error: {}",
        err
    );

    Ok(())
}
