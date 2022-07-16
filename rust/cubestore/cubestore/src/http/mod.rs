pub mod status;

use std::sync::Arc;

use warp::{Filter, Rejection, Reply};

use crate::codegen::http_message_generated::{
    get_root_as_http_message, HttpColumnValue, HttpColumnValueArgs, HttpError, HttpErrorArgs,
    HttpMessageArgs, HttpQuery, HttpQueryArgs, HttpResultSet, HttpResultSetArgs, HttpRow,
    HttpRowArgs, HttpTable, HttpTableArgs,
};
use crate::metastore::{Column, ColumnType, ImportFormat};
use crate::mysql::SqlAuthService;
use crate::sql::{SqlQueryContext, SqlService};
use crate::store::DataFrame;
use crate::table::{Row, TableValue};
use crate::util::WorkerLoop;
use crate::CubeError;
use async_std::fs::File;
use datafusion::cube_ext;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use futures::{AsyncWriteExt, SinkExt, Stream, StreamExt};
use hex::ToHex;
use http_auth_basic::Credentials;
use log::error;
use log::info;
use log::trace;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use warp::filters::ws::{Message, Ws};
use warp::http::StatusCode;
use warp::reject::Reject;

pub struct HttpServer {
    bind_address: String,
    sql_service: Arc<dyn SqlService>,
    auth: Arc<dyn SqlAuthService>,
    worker_loop: WorkerLoop,
    cancel_token: CancellationToken,
}

crate::di_service!(HttpServer, []);

#[derive(Debug)]
pub enum CubeRejection {
    NotAuthorized,
    Internal(String),
}

impl From<CubeError> for warp::reject::Rejection {
    fn from(e: CubeError) -> Self {
        warp::reject::custom(CubeRejection::Internal(e.message.to_string()))
    }
}

#[derive(Deserialize)]
pub struct UploadQuery {
    name: String,
}

impl Reject for CubeRejection {}

impl HttpServer {
    pub fn new(
        bind_address: String,
        auth: Arc<dyn SqlAuthService>,
        sql_service: Arc<dyn SqlService>,
    ) -> Arc<Self> {
        Arc::new(Self {
            bind_address,
            auth,
            sql_service,
            worker_loop: WorkerLoop::new("HttpServer message processing"),
            cancel_token: CancellationToken::new(),
        })
    }

    pub async fn run_server(&self) -> Result<(), CubeError> {
        let (tx, mut rx) =
            mpsc::channel::<(mpsc::Sender<HttpMessage>, SqlQueryContext, HttpMessage)>(100000);
        let auth_service = self.auth.clone();
        let tx_to_move_filter = warp::any().map(move || tx.clone());

        let auth_filter = warp::any()
            .and(warp::header::optional("authorization"))
            .and_then(move |auth_header: Option<String>| {
                let auth_service = auth_service.clone();
                async move {
                    let res = HttpServer::authorize(auth_service, auth_header).await;
                    match res {
                        Ok(user) => Ok(SqlQueryContext {
                            user,
                            inline_tables: Arc::new(HashMap::new()),
                            trace_obj: None,
                        }),
                        Err(_) => Err(warp::reject::custom(CubeRejection::NotAuthorized)),
                    }
                }
            });

        let context_filter = tx_to_move_filter.and(auth_filter.clone());

        let context_filter_to_move = context_filter.clone();

        let query_route = warp::path!("ws")
            .and(context_filter_to_move)
            .and(warp::ws::ws())
            .and_then(|tx: mpsc::Sender<(mpsc::Sender<HttpMessage>, SqlQueryContext, HttpMessage)>, sql_query_context: SqlQueryContext, ws: Ws| async move {
                let tx_to_move = tx.clone();
                let sql_query_context = sql_query_context.clone();
                Result::<_, Rejection>::Ok(ws.on_upgrade(async move |mut web_socket| {
                    let (response_tx, mut response_rx) = mpsc::channel::<HttpMessage>(10000);
                    loop {
                        tokio::select! {
                            Some(res) = response_rx.recv() => {
                                trace!("Sending web socket response");
                                let send_res = web_socket.send(Message::binary(res.bytes())).await;
                                if let Err(e) = send_res {
                                    error!("Websocket message send error: {:?}", e)
                                }
                            }
                            Some(msg) = web_socket.next() => {
                                match msg {
                                    Err(e) => {
                                        error!("Websocket error: {:?}", e);
                                        break;
                                    }
                                    Ok(msg) => {
                                        if msg.is_binary() {
                                            match HttpMessage::read(msg.into_bytes()) {
                                                Err(e) => error!("Websocket message read error: {:?}", e),
                                                Ok(msg) => {
                                                    trace!("Received web socket message");
                                                    let message_id = msg.message_id;
                                                    // TODO use timeout instead of try send for burst control however try_send is safer for now
                                                    if let Err(e) = tx_to_move.try_send((response_tx.clone(), sql_query_context.clone(), msg)) {
                                                        error!("Websocket channel error: {:?}", e);
                                                        let send_res = web_socket.send(
                                                            Message::binary(HttpMessage { message_id, command: HttpCommand::Error { error: e.to_string() } }.bytes())
                                                        ).await;
                                                        if let Err(e) = send_res {
                                                            error!("Websocket message send error: {:?}", e)
                                                        }
                                                        break;
                                                    }
                                                }
                                            };
                                        } else if msg.is_ping() {
                                            let send_res = web_socket.send(Message::pong(Vec::new())).await;
                                            if let Err(e) = send_res {
                                                error!("Websocket ping send error: {:?}", e)
                                            }
                                        } else if msg.is_close() {
                                            break;
                                        } else {
                                            error!("Websocket received non binary msg: {:?}", msg);
                                            break;
                                        }
                                    }
                                }
                            }
                        };
                    };
                }))
            });

        let auth_filter_to_move = auth_filter.clone();
        let sql_service = self.sql_service.clone();

        let upload_route = warp::path!("upload-temp-file")
            .and(auth_filter_to_move)
            .and(warp::query::query::<UploadQuery>())
            .and(warp::body::stream())
            .and_then(move |sql_query_context, upload_query, body| {
                HttpServer::handle_upload(
                    sql_service.clone(),
                    sql_query_context,
                    upload_query,
                    body,
                )
            });

        let sql_service = self.sql_service.clone();

        let addr: SocketAddr = self.bind_address.parse().unwrap();
        info!("Http Server is listening on {}", self.bind_address);
        let process_loop = self.worker_loop.process_channel(
            sql_service,
            &mut rx,
            async move |sql_service,
                        (
                sender,
                sql_query_context,
                HttpMessage {
                    message_id,
                    command,
                },
            )| {
                cube_ext::spawn(async move {
                    let res =
                        HttpServer::process_command(sql_service, sql_query_context, command).await;
                    let message = match res {
                        Ok(command) => HttpMessage {
                            message_id,
                            command,
                        },
                        Err(e) => {
                            log::error!(
                                "Error processing HTTP command: {}\n",
                                e.display_with_backtrace()
                            );
                            HttpMessage {
                                message_id,
                                command: HttpCommand::Error {
                                    error: e.to_string(),
                                },
                            }
                        }
                    };
                    if let Err(e) = sender.send(message).await {
                        error!("Send result channel error: {:?}", e);
                    }
                });
                Ok(())
            },
        );
        let cancel_token = self.cancel_token.clone();
        let (_, server_future) = warp::serve(query_route.or(upload_route).recover(
            |err: Rejection| async move {
                let mut obj = HashMap::new();
                if let Some(ws_error) = err.find::<CubeRejection>() {
                    match ws_error {
                        CubeRejection::NotAuthorized => {
                            obj.insert("error".to_string(), "Not authorized".to_string());
                            Ok(warp::reply::with_status(
                                warp::reply::json(&obj),
                                StatusCode::FORBIDDEN,
                            ))
                        }
                        CubeRejection::Internal(e) => {
                            obj.insert("error".to_string(), e.to_string());
                            Ok(warp::reply::with_status(
                                warp::reply::json(&obj),
                                StatusCode::INTERNAL_SERVER_ERROR,
                            ))
                        }
                    }
                } else {
                    Err(err)
                }
            },
        ))
        .bind_with_graceful_shutdown(addr, async move { cancel_token.cancelled().await });
        let _ = tokio::join!(process_loop, server_future);

        Ok(())
    }

    pub async fn handle_upload(
        sql_service: Arc<dyn SqlService>,
        sql_query_context: SqlQueryContext,
        upload_query: UploadQuery,
        mut body: impl Stream<Item = Result<impl warp::Buf, warp::Error>> + Unpin,
    ) -> Result<impl Reply, Rejection> {
        let temp_file = NamedTempFile::new_in(
            sql_service
                .temp_uploads_dir(sql_query_context.clone())
                .await
                .map_err(|e| CubeRejection::Internal(e.to_string()))?,
        )
        .map_err(|e| CubeRejection::Internal(e.to_string()))?;
        {
            let mut file = File::create(temp_file.path())
                .await
                .map_err(|e| CubeRejection::Internal(e.to_string()))?;
            while let Some(item) = body.next().await {
                let item = item.map_err(|e| CubeRejection::Internal(e.to_string()))?;
                file.write_all(item.chunk())
                    .await
                    .map_err(|e| CubeRejection::Internal(e.to_string()))?;
            }
            file.flush()
                .await
                .map_err(|e| CubeRejection::Internal(e.to_string()))?;
            file.close()
                .await
                .map_err(|e| CubeRejection::Internal(e.to_string()))?;
        }

        sql_service
            .upload_temp_file(sql_query_context, upload_query.name, temp_file.path())
            .await
            .map_err(|e| CubeRejection::Internal(e.to_string()))?;

        Ok(warp::reply())
    }

    pub async fn process_command(
        sql_service: Arc<dyn SqlService>,
        sql_query_context: SqlQueryContext,
        command: HttpCommand,
    ) -> Result<HttpCommand, CubeError> {
        match command {
            HttpCommand::Query {
                query,
                inline_tables,
                trace_obj,
            } => Ok(HttpCommand::ResultSet {
                data_frame: sql_service
                    .exec_query_with_context(
                        sql_query_context
                            .with_trace_obj(trace_obj)
                            .with_inline_tables(inline_tables),
                        &query,
                    )
                    .await?,
            }),
            x => Err(CubeError::user(format!("Unexpected command: {:?}", x))),
        }
    }

    pub async fn authorize(
        auth: Arc<dyn SqlAuthService>,
        auth_header: Option<String>,
    ) -> Result<Option<String>, CubeError> {
        let credentials = auth_header
            .map(|auth_header| Credentials::from_header(auth_header))
            .transpose()
            .map_err(|e| CubeError::from_error(e))?;
        if let Some(password) = auth
            .authenticate(credentials.as_ref().map(|c| c.user_id.to_string()))
            .await?
        {
            if Some(password) != credentials.as_ref().map(|c| c.password.to_string()) {
                Err(CubeError::user(
                    "User or password doesn't match".to_string(),
                ))
            } else {
                Ok(credentials.as_ref().map(|c| c.user_id.to_string()))
            }
        } else {
            Ok(credentials.as_ref().map(|c| c.user_id.to_string()))
        }
    }

    pub async fn stop_processing(&self) {
        self.worker_loop.stop();
        self.cancel_token.cancel();
    }
}

#[derive(Debug, PartialEq)]
pub struct HttpMessage {
    message_id: u32,
    command: HttpCommand,
}

#[derive(Debug, PartialEq)]
pub enum HttpCommand {
    Query {
        query: String,
        inline_tables: Vec<(String, Arc<DataFrame>)>,
        trace_obj: Option<String>,
    },
    ResultSet {
        data_frame: Arc<DataFrame>,
    },
    Error {
        error: String,
    },
}

impl HttpMessage {
    pub fn bytes(&self) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let args = HttpMessageArgs {
            message_id: self.message_id,
            command_type: match self.command {
                HttpCommand::Query { .. } => {
                    crate::codegen::http_message_generated::HttpCommand::HttpQuery
                }
                HttpCommand::ResultSet { .. } => {
                    crate::codegen::http_message_generated::HttpCommand::HttpResultSet
                }
                HttpCommand::Error { .. } => {
                    crate::codegen::http_message_generated::HttpCommand::HttpError
                }
            },
            command: match &self.command {
                HttpCommand::Query {
                    query,
                    inline_tables,
                    trace_obj,
                } => {
                    let query_offset = builder.create_string(&query);
                    let trace_obj_offset = trace_obj.as_ref().map(|o| builder.create_string(o));
                    let mut inline_tables_offsets = Vec::with_capacity(inline_tables.len());
                    for (name, inline_table) in inline_tables.iter() {
                        let name_offset = builder.create_string(&name);
                        let columns_vec =
                            HttpMessage::build_columns(&mut builder, inline_table.clone());
                        let types_vec =
                            HttpMessage::build_types(&mut builder, inline_table.clone());
                        let rows_vec = HttpMessage::build_rows(&mut builder, inline_table.clone());
                        let inline_table_offset = HttpTable::create(
                            &mut builder,
                            &HttpTableArgs {
                                name: Some(name_offset),
                                columns: Some(columns_vec),
                                types: Some(types_vec),
                                rows: Some(rows_vec),
                            },
                        );
                        inline_tables_offsets.push(inline_table_offset);
                    }
                    let inline_tables_offset =
                        builder.create_vector(inline_tables_offsets.as_slice());
                    Some(
                        HttpQuery::create(
                            &mut builder,
                            &HttpQueryArgs {
                                query: Some(query_offset),
                                inline_tables: if inline_tables.is_empty() {
                                    None
                                } else {
                                    Some(inline_tables_offset)
                                },
                                trace_obj: trace_obj_offset,
                            },
                        )
                        .as_union_value(),
                    )
                }
                HttpCommand::Error { error } => {
                    let error_offset = builder.create_string(&error);
                    Some(
                        HttpError::create(
                            &mut builder,
                            &HttpErrorArgs {
                                error: Some(error_offset),
                            },
                        )
                        .as_union_value(),
                    )
                }
                HttpCommand::ResultSet { data_frame } => {
                    let columns_vec = HttpMessage::build_columns(&mut builder, data_frame.clone());
                    let rows = HttpMessage::build_rows(&mut builder, data_frame.clone());

                    Some(
                        HttpResultSet::create(
                            &mut builder,
                            &HttpResultSetArgs {
                                columns: Some(columns_vec),
                                rows: Some(rows),
                            },
                        )
                        .as_union_value(),
                    )
                }
            },
        };
        let message =
            crate::codegen::http_message_generated::HttpMessage::create(&mut builder, &args);
        builder.finish(message, None);
        builder.finished_data().to_vec() // TODO copy
    }

    fn build_columns<'a: 'ma, 'ma>(
        builder: &'ma mut FlatBufferBuilder<'a>,
        data_frame: Arc<DataFrame>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>> {
        let columns = data_frame
            .get_columns()
            .iter()
            .map(|c| c.get_name().as_str())
            .collect::<Vec<_>>();
        let columns_vec = builder.create_vector_of_strings(columns.as_slice());
        columns_vec
    }

    fn build_types<'a: 'ma, 'ma>(
        builder: &'ma mut FlatBufferBuilder<'a>,
        data_frame: Arc<DataFrame>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>> {
        let types = data_frame
            .get_columns()
            .iter()
            .map(|c| c.get_column_type().to_string())
            .collect::<Vec<_>>();
        let str_types = types.iter().map(|t| t.as_str()).collect::<Vec<_>>();
        let types_vec = builder.create_vector_of_strings(str_types.as_slice());
        types_vec
    }

    fn build_rows<'a: 'ma, 'ma>(
        builder: &'ma mut FlatBufferBuilder<'a>,
        data_frame: Arc<DataFrame>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<HttpRow<'a>>>> {
        let mut row_offsets = Vec::with_capacity(data_frame.get_rows().len());
        for row in data_frame.get_rows().iter() {
            let mut value_offsets = Vec::with_capacity(row.values().len());
            for (i, value) in row.values().iter().enumerate() {
                let value = match value {
                    TableValue::Null => HttpColumnValue::create(
                        builder,
                        &HttpColumnValueArgs { string_value: None },
                    ),
                    TableValue::String(v) => {
                        let string_value = Some(builder.create_string(v));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Int(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Decimal(v) => {
                        let scale = u8::try_from(
                            data_frame.get_columns()[i].get_column_type().target_scale(),
                        )
                        .unwrap();
                        let string_value = Some(builder.create_string(&v.to_string(scale)));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Float(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Bytes(v) => {
                        let string_value = Some(
                            builder.create_string(&format!("0x{}", v.encode_hex_upper::<String>())),
                        );
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Timestamp(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Boolean(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                };
                value_offsets.push(value);
            }
            let values = Some(builder.create_vector(value_offsets.as_slice()));
            let row = HttpRow::create(builder, &HttpRowArgs { values });
            row_offsets.push(row);
        }

        let rows = builder.create_vector(row_offsets.as_slice());
        rows
    }

    pub fn read(buffer: Vec<u8>) -> Result<Self, CubeError> {
        let http_message = get_root_as_http_message(buffer.as_slice());
        Ok(HttpMessage {
            message_id: http_message.message_id(),
            command: match http_message.command_type() {
                crate::codegen::http_message_generated::HttpCommand::HttpQuery => {
                    let query = http_message.command_as_http_query().unwrap();
                    let mut inline_tables = Vec::new();
                    if let Some(query_inline_tables) = query.inline_tables() {
                        for inline_table in query_inline_tables.iter() {
                            let name = inline_table.name().unwrap().to_string();
                            let types = inline_table
                                .types()
                                .unwrap()
                                .iter()
                                .map(|column_type| ColumnType::from_string(column_type))
                                .collect::<Result<Vec<_>, CubeError>>()?;
                            let columns = inline_table
                                .columns()
                                .unwrap()
                                .iter()
                                .enumerate()
                                .map(|(i, name)| Column::new(name.to_string(), types[i].clone(), i))
                                .collect::<Vec<_>>();
                            let rows = inline_table
                                .rows()
                                .unwrap()
                                .iter()
                                .map(|r| {
                                    let values = r
                                        .values()
                                        .unwrap()
                                        .iter()
                                        .enumerate()
                                        .map(|(i, v)| {
                                            v.string_value().map_or(Ok(TableValue::Null), |v| {
                                                ImportFormat::parse_column_value_str(&columns[i], v)
                                            })
                                        })
                                        .collect::<Result<Vec<_>, CubeError>>();
                                    values.map(|values| Row::new(values))
                                })
                                .collect::<Result<Vec<_>, CubeError>>()?;
                            inline_tables.push((name, Arc::new(DataFrame::new(columns, rows))));
                        }
                    };
                    HttpCommand::Query {
                        query: query.query().unwrap().to_string(),
                        inline_tables,
                        trace_obj: query.trace_obj().map(|q| q.to_string()),
                    }
                }
                command => {
                    return Err(CubeError::internal(format!(
                        "Unexpected command: {:?}",
                        command
                    )));
                }
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::http::{HttpCommand, HttpMessage};
    use crate::metastore::{Column, ColumnType};
    use crate::sql::timestamp_from_string;
    use crate::store::DataFrame;
    use crate::table::{Row, TableValue};
    use std::sync::Arc;

    #[test]
    fn query_test() {
        let message = HttpMessage {
            message_id: 1234,
            command: HttpCommand::Query {
                query: "test query".to_string(),
                inline_tables: vec![],
                trace_obj: Some("test trace".to_string()),
            },
        };
        let bytes = message.bytes();
        let output_message = HttpMessage::read(bytes).unwrap();
        assert_eq!(message, output_message);
    }

    #[test]
    fn inline_tables_query_test() {
        let columns = vec![
            Column::new("ID".to_string(), ColumnType::Int, 0),
            Column::new("LastName".to_string(), ColumnType::String, 1),
            Column::new("FirstName".to_string(), ColumnType::String, 2),
            Column::new("Timestamp".to_string(), ColumnType::Timestamp, 3),
        ];
        let rows = vec![
            Row::new(vec![
                TableValue::Null,
                TableValue::String("Last 1".to_string()),
                TableValue::String("First 1".to_string()),
                TableValue::Timestamp(timestamp_from_string("2020-01-01T00:00:00.000Z").unwrap()),
            ]),
            Row::new(vec![
                TableValue::Int(2),
                TableValue::Null,
                TableValue::String("First 2".to_string()),
                TableValue::Timestamp(timestamp_from_string("2020-01-02T00:00:00.000Z").unwrap()),
            ]),
            Row::new(vec![
                TableValue::Int(3),
                TableValue::String("Last 3".to_string()),
                TableValue::String("First 3".to_string()),
                TableValue::Timestamp(timestamp_from_string("2020-01-03T00:00:00.000Z").unwrap()),
            ]),
            Row::new(vec![
                TableValue::Int(4),
                TableValue::String("Last 4".to_string()),
                TableValue::String("First 4".to_string()),
                TableValue::Null,
            ]),
        ];
        let data = Arc::new(DataFrame::new(columns, rows.clone()));

        let message = HttpMessage {
            message_id: 1234,
            command: HttpCommand::Query {
                query: "test query".to_string(),
                inline_tables: vec![
                    ("table0".to_string(), data.clone()),
                    ("table1".to_string(), data.clone()),
                ],
                trace_obj: Some("test trace".to_string()),
            },
        };
        let bytes = message.bytes();
        let output_message = HttpMessage::read(bytes).unwrap();
        assert_eq!(message, output_message);
    }
}
