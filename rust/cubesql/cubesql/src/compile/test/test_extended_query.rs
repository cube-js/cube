//! Extended query protocol exchanges that drivers send without a Sync after each step.

use super::test_copy::serve_session;
use bytes::{BufMut, BytesMut};
use pretty_assertions::assert_eq;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// Reading has a timeout so that a server which holds a message back fails the test
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

/// Parse of an unnamed statement without parameter types
async fn parse(socket: &mut TcpStream, sql: &str) {
    let mut payload = BytesMut::new();
    payload.put_u8(0);
    payload.extend_from_slice(sql.as_bytes());
    payload.put_u8(0);
    payload.put_i16(0);
    send(socket, b'P', &payload).await;
}

/// Describe of the unnamed statement
async fn describe_statement(socket: &mut TcpStream) {
    send(socket, b'D', b"S\0").await;
}

/// Bind of the unnamed portal to the unnamed statement, without parameters
async fn bind(socket: &mut TcpStream) {
    let mut payload = BytesMut::new();
    payload.put_u8(0);
    payload.put_u8(0);
    payload.put_i16(0);
    payload.put_i16(0);
    payload.put_i16(0);
    send(socket, b'B', &payload).await;
}

/// Execute of the unnamed portal without a row limit
async fn execute(socket: &mut TcpStream) {
    let mut payload = BytesMut::new();
    payload.put_u8(0);
    payload.put_i32(0);
    send(socket, b'E', &payload).await;
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
    send(&mut socket, b'p', b"test\0").await;
    while read_message(&mut socket).await.0 != b'Z' {}

    socket
}

/// Tags of the messages received up to and including the next one tagged `last`
async fn read_until(socket: &mut TcpStream, last: u8) -> Vec<u8> {
    let mut tags = vec![];
    loop {
        let (tag, _) = read_message(socket).await;
        tags.push(tag);
        if tag == last {
            return tags;
        }
    }
}

#[tokio::test]
async fn test_extended_query_error_is_sent_before_sync() {
    let mut socket = connect(serve_session().await).await;

    // Postgrex prepares a statement with Parse, Describe and Flush, and waits for the
    // answer before it sends Sync
    parse(&mut socket, "SELECT * FROM no_such_table").await;
    describe_statement(&mut socket).await;
    send(&mut socket, b'H', b"").await;

    let (tag, body) = read_message(&mut socket).await;
    assert_eq!(tag, b'E', "server must answer the Flush with the error");
    assert!(
        String::from_utf8_lossy(&body).contains("no_such_table"),
        "error must name the table: {}",
        String::from_utf8_lossy(&body)
    );

    send(&mut socket, b'S', b"").await;
    assert_eq!(read_until(&mut socket, b'Z').await, vec![b'Z']);

    // The connection is usable again after the Sync
    parse(&mut socket, "SELECT 1").await;
    describe_statement(&mut socket).await;
    send(&mut socket, b'H', b"").await;
    assert_eq!(read_until(&mut socket, b'T').await, vec![b'1', b't', b'T']);

    send(&mut socket, b'S', b"").await;
    assert_eq!(read_until(&mut socket, b'Z').await, vec![b'Z']);
}

#[tokio::test]
async fn test_extended_query_skips_messages_after_an_error_until_sync() {
    let mut socket = connect(serve_session().await).await;

    parse(&mut socket, "SELECT * FROM no_such_table").await;
    bind(&mut socket).await;
    describe_statement(&mut socket).await;
    execute(&mut socket).await;
    send(&mut socket, b'S', b"").await;

    // One error for the Parse; Bind, Describe and Execute are skipped
    assert_eq!(read_until(&mut socket, b'Z').await, vec![b'E', b'Z']);

    parse(&mut socket, "SELECT 1").await;
    bind(&mut socket).await;
    execute(&mut socket).await;
    send(&mut socket, b'S', b"").await;
    assert_eq!(
        read_until(&mut socket, b'Z').await,
        vec![b'1', b'2', b'D', b'C', b'Z']
    );
}
