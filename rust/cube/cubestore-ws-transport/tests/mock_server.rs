//! Spin up a minimal tokio-tungstenite server that mimics cubestore's /ws endpoint
//! and verify the client's happy path: handshake, message_id correlation,
//! HttpResultSet decoding, and HttpError surfacing as `TransportError::Query`.

use std::time::Duration;

use cubeshared::codegen::{
    root_as_http_message, HttpColumnValueArgs, HttpCommand as FbCommand, HttpError as FbError,
    HttpErrorArgs, HttpMessage, HttpMessageArgs, HttpResultSet as FbResultSet, HttpResultSetArgs,
    HttpRow as FbRow, HttpRowArgs,
};
use cubestore_ws_transport::{Client, ClientConfig, QueryResult, ResultData, TransportError};
use flatbuffers::FlatBufferBuilder;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Pull legacy stringified rows out of a `QueryResult`. The mock server always
/// emits the legacy `HttpResultSet` envelope, so this is exhaustive enough for
/// the tests in this file.
fn legacy_rows(r: &QueryResult) -> &Vec<Vec<Option<String>>> {
    match &r.data {
        ResultData::Legacy { rows, .. } => rows,
        ResultData::Arrow { .. } => panic!("expected ResultData::Legacy, got Arrow"),
    }
}

fn build_result_set(message_id: u32, connection_id: &str) -> bytes::Bytes {
    let mut b = FlatBufferBuilder::with_capacity(1024);

    // columns: ["id", "name"]
    let id_col = b.create_string("id");
    let name_col = b.create_string("name");
    let cols_vec = b.create_vector(&[id_col, name_col]);

    use cubeshared::codegen::HttpColumnValue as Cv;

    // row 1: ["1", "alice"]
    let s1 = b.create_string("1");
    let v1_1 = Cv::create(
        &mut b,
        &HttpColumnValueArgs {
            string_value: Some(s1),
        },
    );
    let s2 = b.create_string("alice");
    let v1_2 = Cv::create(
        &mut b,
        &HttpColumnValueArgs {
            string_value: Some(s2),
        },
    );
    let r1_vals = b.create_vector(&[v1_1, v1_2]);
    let r1 = FbRow::create(
        &mut b,
        &HttpRowArgs {
            values: Some(r1_vals),
        },
    );

    // row 2: ["2", "bob"]
    let s3 = b.create_string("2");
    let v2_1 = Cv::create(
        &mut b,
        &HttpColumnValueArgs {
            string_value: Some(s3),
        },
    );
    let s4 = b.create_string("bob");
    let v2_2 = Cv::create(
        &mut b,
        &HttpColumnValueArgs {
            string_value: Some(s4),
        },
    );
    let r2_vals = b.create_vector(&[v2_1, v2_2]);
    let r2 = FbRow::create(
        &mut b,
        &HttpRowArgs {
            values: Some(r2_vals),
        },
    );

    let rows_vec = b.create_vector(&[r1, r2]);

    let rs = FbResultSet::create(
        &mut b,
        &HttpResultSetArgs {
            columns: Some(cols_vec),
            rows: Some(rows_vec),
        },
    );

    let conn_off = b.create_string(connection_id);
    let msg = HttpMessage::create(
        &mut b,
        &HttpMessageArgs {
            message_id,
            command_type: FbCommand::HttpResultSet,
            command: Some(rs.as_union_value()),
            connection_id: Some(conn_off),
        },
    );
    b.finish(msg, None);
    bytes::Bytes::copy_from_slice(b.finished_data())
}

fn build_error(message_id: u32, connection_id: &str, error: &str) -> bytes::Bytes {
    let mut b = FlatBufferBuilder::with_capacity(256);
    let err_off = b.create_string(error);
    let err = FbError::create(
        &mut b,
        &HttpErrorArgs {
            error: Some(err_off),
        },
    );
    let conn_off = b.create_string(connection_id);
    let msg = HttpMessage::create(
        &mut b,
        &HttpMessageArgs {
            message_id,
            command_type: FbCommand::HttpError,
            command: Some(err.as_union_value()),
            connection_id: Some(conn_off),
        },
    );
    b.finish(msg, None);
    bytes::Bytes::copy_from_slice(b.finished_data())
}

/// Boot a one-shot mock server. Returns the bound port. The server replies to every
/// HttpQuery with either a canned result set or, when the SQL starts with `ERR `,
/// an HttpError carrying the rest of the SQL as the message.
async fn boot_mock_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (mut sink, mut src) = ws_stream.split();
                while let Some(msg) = src.next().await {
                    let Ok(msg) = msg else { break };
                    if !msg.is_binary() {
                        continue;
                    }
                    let bytes = msg.into_data();
                    let parsed = root_as_http_message(&bytes).expect("parse client message");
                    let msg_id = parsed.message_id();
                    let conn_id = parsed.connection_id().unwrap_or("").to_string();
                    let q = parsed
                        .command_as_http_query()
                        .expect("expected HttpQuery from client");
                    let sql = q.query().unwrap_or("").to_string();

                    let reply = if let Some(rest) = sql.strip_prefix("ERR ") {
                        build_error(msg_id, &conn_id, rest)
                    } else {
                        build_result_set(msg_id, &conn_id)
                    };
                    if sink.send(Message::Binary(reply)).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    port
}

#[tokio::test]
async fn happy_path_query_returns_rows() -> Result<(), TransportError> {
    let port = boot_mock_server().await;
    let url = url::Url::parse(&format!("ws://127.0.0.1:{port}/")).unwrap();
    let mut cfg = ClientConfig::new(url);
    cfg.connect_timeout = Duration::from_secs(2);
    let client = Client::connect(cfg).await?;

    let result = client.query("SELECT * FROM whatever").await?;

    assert_eq!(
        result.get_columns(),
        vec!["id".to_string(), "name".to_string()]
    );
    let rows = legacy_rows(&result);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_deref(), Some("1"));
    assert_eq!(rows[0][1].as_deref(), Some("alice"));
    assert_eq!(rows[1][0].as_deref(), Some("2"));
    assert_eq!(rows[1][1].as_deref(), Some("bob"));

    Ok(())
}

/// Full WS round-trip with a 12-column result — guards against any layer in the
/// stack accidentally dropping columns past the first few.
#[tokio::test]
async fn wide_result_full_round_trip() -> Result<(), TransportError> {
    use cubeshared::codegen::{HttpColumnValue as Cv, HttpMessage as FbMsg, HttpMessageArgs};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (mut sink, mut src) = ws.split();
                while let Some(msg) = src.next().await {
                    let Ok(msg) = msg else { break };
                    if !msg.is_binary() {
                        continue;
                    }
                    let bytes = msg.into_data();
                    let parsed = cubeshared::codegen::root_as_http_message(&bytes).unwrap();
                    let msg_id = parsed.message_id();
                    let conn_id = parsed.connection_id().unwrap_or("").to_string();

                    let mut b = FlatBufferBuilder::with_capacity(2048);
                    let names = [
                        "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11",
                    ];
                    let col_offs: Vec<_> = names.iter().map(|n| b.create_string(n)).collect();
                    let cols = b.create_vector(&col_offs);

                    let cells: Vec<_> = (0..names.len())
                        .map(|i| {
                            let s = b.create_string(&format!("v{i}"));
                            Cv::create(
                                &mut b,
                                &HttpColumnValueArgs {
                                    string_value: Some(s),
                                },
                            )
                        })
                        .collect();
                    let vals = b.create_vector(&cells);
                    let row = FbRow::create(&mut b, &HttpRowArgs { values: Some(vals) });
                    let rows = b.create_vector(&[row]);
                    let rs = FbResultSet::create(
                        &mut b,
                        &HttpResultSetArgs {
                            columns: Some(cols),
                            rows: Some(rows),
                        },
                    );
                    let conn = b.create_string(&conn_id);
                    let msg = FbMsg::create(
                        &mut b,
                        &HttpMessageArgs {
                            message_id: msg_id,
                            command_type: FbCommand::HttpResultSet,
                            command: Some(rs.as_union_value()),
                            connection_id: Some(conn),
                        },
                    );
                    b.finish(msg, None);
                    let _ = sink
                        .send(Message::Binary(bytes::Bytes::copy_from_slice(
                            b.finished_data(),
                        )))
                        .await;
                }
            });
        }
    });

    let url = url::Url::parse(&format!("ws://127.0.0.1:{port}/")).unwrap();
    let mut cfg = ClientConfig::new(url);
    cfg.connect_timeout = Duration::from_secs(2);
    let client = Client::connect(cfg).await?;

    let result = client.query("SELECT *").await?;
    assert_eq!(
        result.get_columns(),
        ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "all 12 columns should survive WS round-trip"
    );
    let rows = legacy_rows(&result);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 12);

    Ok(())
}

#[tokio::test]
async fn server_error_surfaces_as_query_error() -> Result<(), TransportError> {
    let port = boot_mock_server().await;
    let url = url::Url::parse(&format!("ws://127.0.0.1:{port}/")).unwrap();
    let mut cfg = ClientConfig::new(url);
    cfg.connect_timeout = Duration::from_secs(2);
    let client = Client::connect(cfg).await?;

    let err = client.query("ERR boom: bad sql").await.unwrap_err();
    match err {
        TransportError::Query(msg) => assert_eq!(msg, "boom: bad sql"),
        other => panic!("expected Query error, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn many_queries_correlate_by_message_id() -> Result<(), TransportError> {
    let port = boot_mock_server().await;
    let url = url::Url::parse(&format!("ws://127.0.0.1:{port}/")).unwrap();
    let mut cfg = ClientConfig::new(url);
    cfg.connect_timeout = Duration::from_secs(2);
    let client = Client::connect(cfg).await?;

    let mut handles = Vec::new();
    for i in 0..10 {
        let c = client.clone();
        handles.push(tokio::spawn(
            async move { c.query(format!("SELECT {i}")).await },
        ));
    }
    for h in handles {
        let r = h.await.unwrap()?;
        assert_eq!(legacy_rows(&r).len(), 2);
    }

    Ok(())
}
