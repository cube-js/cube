use crate::{
    compile::{test::TestContext, DatabaseProtocol},
    sql::postgres::shim::AsyncPostgresShim,
    telemetry::SessionLogger,
};
use bytes::{BufMut, BytesMut};
use pretty_assertions::assert_eq;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

/// Serve a single connection and return a socket connected to it, past authentication.
async fn connect() -> TcpStream {
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

    assert_eq!(read_message(&mut socket).await, b'R');
    socket
        .write_all(&message(b'p', b"test\0"))
        .await
        .expect("must write");
    while read_message(&mut socket).await != b'Z' {}

    socket
}

/// Reading has a timeout so that a server which never answers fails the test
/// instead of hanging it.
async fn read_message(socket: &mut TcpStream) -> u8 {
    let read = async {
        let tag = socket.read_u8().await.expect("must read a message tag");
        let length = socket.read_u32().await.expect("must read a message length") as usize;
        let mut body = vec![0; length - 4];
        socket
            .read_exact(&mut body)
            .await
            .expect("must read a message body");

        tag
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

/// Send Parse, Bind, Execute and Sync for `query` and return the tags of the
/// messages received up to ReadyForQuery.
async fn extended_query(socket: &mut TcpStream, query: &str) -> Vec<char> {
    // Parse the unnamed statement, no parameter types
    let mut parse = BytesMut::new();
    parse.extend_from_slice(format!("\0{query}\0").as_bytes());
    parse.put_i16(0);
    // Bind the unnamed portal, no parameters, no result formats
    let mut bind = BytesMut::new();
    bind.extend_from_slice(b"\0\0");
    bind.put_i16(0);
    bind.put_i16(0);
    bind.put_i16(0);
    // Execute the unnamed portal, no row limit
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
        let tag = read_message(socket).await;
        tags.push(tag as char);
        if tag == b'Z' {
            break;
        }
    }
    tags
}

/// After a failed Parse the backend must discard every message until Sync, so a
/// following Bind must not answer BindComplete, and the connection must work again.
#[tokio::test]
async fn test_bind_after_failed_parse_is_discarded() {
    for query in ["SELEC 1", "SELECT * FROM unknown_table"] {
        let mut socket = connect().await;

        assert_eq!(
            extended_query(&mut socket, query).await,
            vec!['E', 'Z'],
            "{query}"
        );
        assert_eq!(
            extended_query(&mut socket, "SELECT 1").await,
            vec!['1', '2', 'D', 'C', 'Z'],
            "{query}"
        );
    }
}
