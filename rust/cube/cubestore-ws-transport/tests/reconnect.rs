//! Verify reconnect + in-flight resend: the first connection drops mid-query,
//! and the actor re-establishes the WS, resubmits the original buffer (preserving
//! its message_id), and resolves the pending oneshot from the second connection.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cubeshared::codegen::{
    root_as_http_message, HttpColumnValueArgs, HttpCommand as FbCommand, HttpMessage,
    HttpMessageArgs, HttpResultSet as FbResultSet, HttpResultSetArgs, HttpRow as FbRow,
    HttpRowArgs,
};
use cubestore_ws_transport::{Client, ClientConfig};
use flatbuffers::FlatBufferBuilder;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::protocol::Message;

fn build_result_set(message_id: u32, connection_id: &str) -> bytes::Bytes {
    let mut b = FlatBufferBuilder::with_capacity(256);
    let col = b.create_string("value");
    let cols = b.create_vector(&[col]);
    let cell = b.create_string("ok");
    use cubeshared::codegen::HttpColumnValue as Cv;
    let v = Cv::create(
        &mut b,
        &HttpColumnValueArgs {
            string_value: Some(cell),
        },
    );
    let row_vals = b.create_vector(&[v]);
    let row = FbRow::create(
        &mut b,
        &HttpRowArgs {
            values: Some(row_vals),
        },
    );
    let rows = b.create_vector(&[row]);
    let rs = FbResultSet::create(
        &mut b,
        &HttpResultSetArgs {
            columns: Some(cols),
            rows: Some(rows),
        },
    );
    let conn = b.create_string(connection_id);
    let msg = HttpMessage::create(
        &mut b,
        &HttpMessageArgs {
            message_id,
            command_type: FbCommand::HttpResultSet,
            command: Some(rs.as_union_value()),
            connection_id: Some(conn),
        },
    );
    b.finish(msg, None);
    bytes::Bytes::copy_from_slice(b.finished_data())
}

#[tokio::test]
async fn resends_in_flight_query_after_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    // Track which connection this is for the running server task.
    let connection_count = Arc::new(AtomicU32::new(0));
    // Capture the message_id observed across connections — must match.
    let observed_msg_ids = Arc::new(tokio::sync::Mutex::new(Vec::<u32>::new()));

    let observed_msg_ids_clone = observed_msg_ids.clone();
    let connection_count_clone = connection_count.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let nth = connection_count_clone.fetch_add(1, Ordering::SeqCst);
            let observed = observed_msg_ids_clone.clone();
            tokio::spawn(async move {
                let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (mut sink, mut src) = ws.split();
                while let Some(msg) = src.next().await {
                    let Ok(msg) = msg else { break };
                    if !msg.is_binary() {
                        continue;
                    }
                    let bytes = msg.into_data();
                    let parsed = root_as_http_message(&bytes).expect("parse");
                    let msg_id = parsed.message_id();
                    let conn_id = parsed.connection_id().unwrap_or("").to_string();
                    observed.lock().await.push(msg_id);

                    if nth == 0 {
                        // First connection: receive the query, then close abruptly without
                        // replying. The client should reconnect and resend.
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    }

                    // Subsequent connections: echo back the result set.
                    let reply = build_result_set(msg_id, &conn_id);
                    let _ = sink.send(Message::Binary(reply)).await;
                }
            });
        }
    });

    let url = url::Url::parse(&format!("ws://127.0.0.1:{port}/")).unwrap();
    let mut cfg = ClientConfig::new(url);
    cfg.connect_timeout = Duration::from_secs(2);
    cfg.max_connect_retries = 5;
    let client = Client::connect(cfg).await.expect("initial connect");

    // The reconnect backoff is (attempt+1)*1000ms; allow generous headroom.
    let result = tokio::time::timeout(Duration::from_secs(5), client.query("SELECT 1"))
        .await
        .expect("query did not complete within timeout")
        .expect("query result");

    assert_eq!(result.columns, vec!["value".to_string()]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].as_deref(), Some("ok"));

    // Both connections should have seen the *same* message id — the resend preserves it.
    let ids = observed_msg_ids.lock().await.clone();
    assert!(ids.len() >= 2, "expected resend, got ids: {ids:?}");
    assert_eq!(ids[0], ids[1], "resent message_id must match the original");
}
