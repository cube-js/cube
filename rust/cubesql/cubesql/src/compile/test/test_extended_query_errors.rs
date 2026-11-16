//! Reproduces https://github.com/cube-js/cube/issues/10962: after a failed Parse, the
//! server still answers the following Bind with BindComplete. Postgres discards every
//! extended query message until Sync once a message has failed, so clients such as
//! npgsql expect only ErrorResponse and ReadyForQuery and break on the extra
//! BindComplete.

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

async fn serve() -> u16 {
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

    port
}

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

async fn connect(port: u16) -> TcpStream {
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

    // Skip the authentication result, parameter statuses and the key data
    while read_message(&mut socket).await.0 != b'Z' {}

    socket
}

#[tokio::test]
async fn test_bind_after_failed_parse_is_skipped() {
    let port = serve().await;
    let mut socket = connect(port).await;

    // Parse (unnamed statement, no parameter types) of a query that fails to plan
    let mut parse = BytesMut::new();
    parse.put_u8(0);
    parse.extend_from_slice(b"SELECT nonexistent_column FROM nonexistent_table\0");
    parse.put_u16(0);

    // Bind of the unnamed portal to the unnamed statement: no parameter formats,
    // no parameters, no result formats
    let mut bind = BytesMut::new();
    bind.put_u8(0);
    bind.put_u8(0);
    bind.put_u16(0);
    bind.put_u16(0);
    bind.put_u16(0);

    // Describe and Execute the unnamed portal
    let describe: &[u8] = b"P\0";
    let mut execute = BytesMut::new();
    execute.put_u8(0);
    execute.put_u32(0);

    let mut batch = BytesMut::new();
    batch.extend_from_slice(&message(b'P', &parse));
    batch.extend_from_slice(&message(b'B', &bind));
    batch.extend_from_slice(&message(b'D', describe));
    batch.extend_from_slice(&message(b'E', &execute));
    batch.extend_from_slice(&message(b'S', &[]));
    socket.write_all(&batch).await.expect("must write");

    let mut tags = vec![];
    loop {
        let (tag, _) = read_message(&mut socket).await;
        tags.push(tag as char);
        if tag == b'Z' {
            break;
        }
    }

    assert_eq!(
        tags.into_iter().collect::<String>(),
        "EZ",
        "only ErrorResponse and ReadyForQuery must follow a failed Parse"
    );
}
