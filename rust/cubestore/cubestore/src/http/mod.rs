pub mod status;

use std::sync::Arc;

use warp::{Filter, Rejection, Reply};

use crate::metastore::{Column, ColumnType, ImportFormat};
use crate::mysql::SqlAuthService;
use crate::sql::{InlineTable, InlineTables, SqlQueryContext, SqlService};
use crate::store::DataFrame;
use crate::table::{Row, TableValue};
use crate::util::WorkerLoop;
use crate::CubeError;
use async_std::fs::File;
use cubeshared::codegen::{
    root_as_http_message, HttpColumnValue, HttpColumnValueArgs, HttpError, HttpErrorArgs,
    HttpMessageArgs, HttpQuery, HttpQueryArgs, HttpResultSet, HttpResultSetArgs, HttpRow,
    HttpRowArgs,
};
use datafusion::cube_ext;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use futures::{AsyncWriteExt, SinkExt, Stream, StreamExt};
use futures_timer::Delay;
use hex::ToHex;
use http_auth_basic::Credentials;
use log::error;
use log::info;
use log::trace;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};
use tempfile::NamedTempFile;
use tokio::io::BufReader;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use warp::filters::ws::{Message, Ws};
use warp::http::StatusCode;
use warp::reject::Reject;

pub struct HttpServer {
    bind_address: String,
    sql_service: Arc<dyn SqlService>,
    auth: Arc<dyn SqlAuthService>,
    check_orphaned_messages_interval: Duration,
    drop_processing_messages_after: Duration,
    drop_complete_messages_after: Duration,
    worker_loop: WorkerLoop,
    drop_orphaned_messages_loop: WorkerLoop,
    cancel_token: CancellationToken,
    max_message_size: usize,
    max_frame_size: usize,
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
        check_orphaned_messages_interval: Duration,
        drop_processing_messages_after: Duration,
        drop_complete_messages_after: Duration,
        max_message_size: usize,
        max_frame_size: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            bind_address,
            auth,
            sql_service,
            check_orphaned_messages_interval,
            drop_processing_messages_after,
            drop_complete_messages_after,
            max_message_size,
            max_frame_size,
            worker_loop: WorkerLoop::new("HttpServer message processing"),
            drop_orphaned_messages_loop: WorkerLoop::new("HttpServer drop orphaned messages"),
            cancel_token: CancellationToken::new(),
        })
    }

    pub async fn run_server(&self) -> Result<(), CubeError> {
        let (tx, mut rx) =
            mpsc::channel::<(mpsc::Sender<Arc<HttpMessage>>, SqlQueryContext, HttpMessage)>(100000);
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
                            inline_tables: InlineTables::new(),
                            trace_obj: None,
                        }),
                        Err(_) => Err(warp::reject::custom(CubeRejection::NotAuthorized)),
                    }
                }
            });

        let context_filter = tx_to_move_filter.and(auth_filter.clone());

        let context_filter_to_move = context_filter.clone();
        let max_frame_size = self.max_frame_size.clone();
        let max_message_size = self.max_message_size.clone();

        let query_route = warp::path!("ws")
            .and(context_filter_to_move)
            .and(warp::ws::ws())
            .and_then(move |tx: mpsc::Sender<(mpsc::Sender<Arc<HttpMessage>>, SqlQueryContext, HttpMessage)>, sql_query_context: SqlQueryContext, ws: Ws| async move {
                let tx_to_move = tx.clone();
                let sql_query_context = sql_query_context.clone();
                Result::<_, Rejection>::Ok(ws.max_frame_size(max_frame_size).max_message_size(max_message_size).on_upgrade(async move |mut web_socket| {
                    let (response_tx, mut response_rx) = mpsc::channel::<Arc<HttpMessage>>(10000);
                    loop {
                        tokio::select! {
                            Some(res) = response_rx.recv() => {
                                trace!("Sending web socket response");
                                let send_res = web_socket.send(Message::binary(res.bytes())).await;
                                if let Err(e) = send_res {
                                    error!("Websocket message send error: {:?}", e)
                                }
                                if res.should_close_connection() {
                                   log::warn!("Websocket connection closed");
                                   break;
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
                                            match HttpMessage::read(msg.into_bytes()).await {
                                                Err(e) => error!("Websocket message read error: {:?}", e),
                                                Ok(msg) => {
                                                    trace!("Received web socket message");
                                                    let message_id = msg.message_id;
                                                    let connection_id = msg.connection_id.clone();
                                                    // TODO use timeout instead of try send for burst control however try_send is safer for now
                                                    if let Err(e) = tx_to_move.try_send((response_tx.clone(), sql_query_context.clone(), msg)) {
                                                        error!("Websocket channel error: {:?}", e);
                                                        let send_res = web_socket.send(
                                                            Message::binary(HttpMessage { message_id, connection_id, command: HttpCommand::Error { error: e.to_string() } }.bytes())
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
        pub enum ProcessingState {
            Processing {
                subscribed_senders: Vec<Sender<Arc<HttpMessage>>>,
                last_touch: SystemTime,
            },
            Complete {
                result: Arc<HttpMessage>,
                last_touch: SystemTime,
            },
        }

        let messages_state = Arc::new(Mutex::new(
            HashMap::<(Option<String>, u32), ProcessingState>::new(),
        ));
        let process_loop = self.worker_loop.process_channel(
            Arc::new((sql_service, messages_state.clone())),
            &mut rx,
            async move |service,
                        (
                sender,
                sql_query_context,
                HttpMessage {
                    message_id,
                    connection_id,
                    command,
                },
            )| {
                let (sql_service, messages_state) = service.as_ref();
                let sql_service = sql_service.clone();
                let messages_state = messages_state.clone();
                if connection_id.is_some() {
                    cube_ext::spawn(async move {
                        let key = (connection_id.clone(), message_id);
                        {
                            let mut messages = messages_state.lock().await;
                            let state = messages.get_mut(&key);
                            match state {
                                None => {
                                    messages.insert(key.clone(), ProcessingState::Processing { subscribed_senders: vec![sender], last_touch: SystemTime::now() });
                                }
                                Some(ProcessingState::Processing { subscribed_senders, .. }) => {
                                    subscribed_senders.push(sender);
                                    return;
                                }
                                Some(ProcessingState::Complete { result, .. }) => {
                                    if let Err(e) = sender.send(result.clone()).await {
                                        error!("Websocket send completed message error: {:?}", e);
                                    } else {
                                        messages.remove(&key);
                                    }
                                    return;
                                }
                            }
                        };
                        let res = HttpServer::process_command(
                            sql_service.clone(),
                            sql_query_context,
                            command.clone(),
                        )
                            .await;
                        let message = Arc::new(match res {
                            Ok(command) => HttpMessage {
                                message_id,
                                connection_id,
                                command,
                            },
                            Err(e) => {
                                log::error!(
                                "Error processing HTTP command: {}\n",
                                e.display_with_backtrace()
                            );
                                let command = if e.is_wrong_connection() {
                                    HttpCommand::CloseConnection {
                                        error: e.to_string(),
                                    }

                                } else {
                                    HttpCommand::Error {
                                        error: e.to_string(),
                                    }
                                };

                                HttpMessage {
                                    message_id,
                                    connection_id,
                                    command,
                                }
                            }
                        });
                        let senders = {
                            let mut messages = messages_state.lock().await;
                            match messages.remove(&key) {
                                None => {
                                    trace!("Websocket message with '{:?}' key was already resolved: {:?}", key, command);
                                    return;
                                }
                                Some(ProcessingState::Processing { subscribed_senders, .. }) => {
                                    messages.insert(key.clone(), ProcessingState::Complete { result: message.clone(), last_touch: SystemTime::now() });
                                    subscribed_senders
                                }
                                Some(ProcessingState::Complete { .. }) => {
                                    trace!("Websocket message with '{:?}' key was already completed by another process: {:?}", key, command);
                                    return;
                                }
                            }
                        };
                        let mut sent_successfully = false;
                        for sender in senders.into_iter() {
                            if sender.is_closed() {
                                trace!("Websocket is closed. Skipping send for '{:?}' key: {:?}", key, command);
                                continue;
                            }
                            if let Err(e) = sender.send(message.clone()).await {
                                error!("Websocket send error. Skipping send for '{:?}' key: {:?}, {}", key, command, e);
                                continue;
                            }
                            sent_successfully = true;
                        }

                        {
                            let mut messages = messages_state.lock().await;
                            match messages.get(&key) {
                                None => {
                                    trace!("Websocket message was resolved just after send. Skipping send for '{:?}' key: {:?}", key, command);
                                    return;
                                }
                                Some(ProcessingState::Processing { .. }) => {
                                    trace!("Websocket message with '{:?}' key was switched to processing just after send: {:?}", key, command);
                                }
                                Some(ProcessingState::Complete { .. }) => {
                                    if sent_successfully {
                                        messages.remove(&key);
                                    }
                                }
                            }
                        }
                    });
                } else {
                    cube_ext::spawn(async move {
                        let res = HttpServer::process_command(
                            sql_service.clone(),
                            sql_query_context,
                            command,
                        )
                            .await;
                        let message = Arc::new(match res {
                            Ok(command) => HttpMessage {
                                message_id,
                                connection_id,
                                command,
                            },
                            Err(e) => {
                                log::error!(
                                "Error processing HTTP command: {}\n",
                                e.display_with_backtrace()
                            );
                                HttpMessage {
                                    message_id,
                                    connection_id,
                                    command: HttpCommand::Error {
                                        error: e.to_string(),
                                    },
                                }
                            }
                        });
                        if sender.is_closed() {
                            trace!(
                                "Websocket is closed. Dropping message with id: {:?}",
                                message_id
                            );
                            return;
                        }
                        if let Err(e) = sender.send(message).await {
                            error!("Websocket send result channel error: {:?}", e);
                        }
                    });
                }
                Ok(())
            },
        );

        let check_orphaned_messages_interval = self.check_orphaned_messages_interval.clone();
        let drop_complete_messages_after = self.drop_complete_messages_after.clone();
        let drop_processing_messages_after = self.drop_processing_messages_after.clone();
        let drop_orphaned_messages_loop = self.drop_orphaned_messages_loop.process(
            messages_state,
            move |_| async move { Ok(Delay::new(check_orphaned_messages_interval.clone()).await) },
            move |messages_state, _| async move {
                let mut messages_state = messages_state.lock().await;
                let mut keys_to_remove = Vec::new();
                let mut orphaned_complete_results = 0;
                for (key, state) in messages_state.iter() {
                    match state {
                        ProcessingState::Processing { last_touch, .. } => {
                            if SystemTime::now()
                                .duration_since(last_touch.clone())
                                .unwrap()
                                > drop_processing_messages_after
                            {
                                trace!("Removing orphaned processing message with '{:?}' key", key);
                                keys_to_remove.push(key.clone());
                            }
                        }
                        ProcessingState::Complete { last_touch, .. } => {
                            if SystemTime::now()
                                .duration_since(last_touch.clone())
                                .unwrap()
                                > drop_complete_messages_after
                            {
                                trace!("Removing orphaned complete message with '{:?}' key", key);
                                keys_to_remove.push(key.clone());
                            } else {
                                orphaned_complete_results += 1;
                            }
                        }
                    }
                }
                if orphaned_complete_results > 100 {
                    log::warn!(
                        "Keeping {} orphaned complete results to be retrieved by reconnecting socket",
                        orphaned_complete_results
                    );
                }
                for key in keys_to_remove {
                    messages_state.remove(&key);
                }
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
        let _ = tokio::join!(process_loop, server_future, drop_orphaned_messages_loop);

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
                            .with_inline_tables(&inline_tables),
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
        self.drop_orphaned_messages_loop.stop();
        self.cancel_token.cancel();
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HttpMessage {
    message_id: u32,
    command: HttpCommand,
    connection_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HttpCommand {
    Query {
        query: String,
        inline_tables: InlineTables,
        trace_obj: Option<String>,
    },
    ResultSet {
        data_frame: Arc<DataFrame>,
    },
    CloseConnection {
        error: String,
    },
    Error {
        error: String,
    },
}

impl HttpMessage {
    pub fn bytes(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::with_capacity(1024);
        let args = HttpMessageArgs {
            message_id: self.message_id,
            command_type: match self.command {
                HttpCommand::Query { .. } => cubeshared::codegen::HttpCommand::HttpQuery,
                HttpCommand::ResultSet { .. } => cubeshared::codegen::HttpCommand::HttpResultSet,
                HttpCommand::CloseConnection { .. } | HttpCommand::Error { .. } => {
                    cubeshared::codegen::HttpCommand::HttpError
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
                    if !inline_tables.is_empty() {
                        panic!("Not implemented")
                    }
                    Some(
                        HttpQuery::create(
                            &mut builder,
                            &HttpQueryArgs {
                                query: Some(query_offset),
                                inline_tables: None,
                                trace_obj: trace_obj_offset,
                            },
                        )
                        .as_union_value(),
                    )
                }
                HttpCommand::Error { error } | HttpCommand::CloseConnection { error } => {
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
                    let columns_vec =
                        HttpMessage::build_columns(&mut builder, data_frame.get_columns());
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
            connection_id: self
                .connection_id
                .as_ref()
                .map(|c| builder.create_string(c)),
        };
        let message = cubeshared::codegen::HttpMessage::create(&mut builder, &args);
        builder.finish(message, None);
        builder.finished_data().to_vec() // TODO copy
    }

    pub fn should_close_connection(&self) -> bool {
        matches!(self.command, HttpCommand::CloseConnection { .. })
    }

    fn build_columns<'a: 'ma, 'ma>(
        builder: &'ma mut FlatBufferBuilder<'a>,
        columns: &Vec<Column>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>> {
        let columns = columns
            .iter()
            .map(|c| builder.create_string(c.get_name()))
            .collect::<Vec<_>>();
        let columns_vec = builder.create_vector(columns.as_slice());
        columns_vec
    }

    fn build_rows<'a: 'ma, 'ma>(
        builder: &'ma mut FlatBufferBuilder<'a>,
        data_frame: Arc<DataFrame>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<HttpRow<'a>>>> {
        let columns = data_frame.get_columns();
        let rows = data_frame.get_rows();
        let mut row_offsets = Vec::with_capacity(rows.len());
        for row in rows.iter() {
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
                    TableValue::Int96(v) => {
                        let string_value = Some(builder.create_string(&v.to_string()));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Decimal(v) => {
                        let scale =
                            u8::try_from(columns[i].get_column_type().target_scale()).unwrap();
                        let string_value = Some(builder.create_string(&v.to_string(scale)));
                        HttpColumnValue::create(builder, &HttpColumnValueArgs { string_value })
                    }
                    TableValue::Decimal96(v) => {
                        let scale =
                            u8::try_from(columns[i].get_column_type().target_scale()).unwrap();
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

    pub async fn read(buffer: Vec<u8>) -> Result<Self, CubeError> {
        let http_message = root_as_http_message(buffer.as_slice())?;
        Ok(HttpMessage {
            message_id: http_message.message_id(),
            connection_id: http_message.connection_id().map(|s| s.to_string()),
            command: match http_message.command_type() {
                cubeshared::codegen::HttpCommand::HttpQuery => {
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
                            let rows = if inline_table.csv_rows().is_some() {
                                let csv_rows = inline_table.csv_rows().unwrap().to_owned();
                                let csv_reader = Box::pin(BufReader::new(csv_rows.as_bytes()));
                                let mut rows_stream = ImportFormat::CSVNoHeader
                                    .row_stream_from_reader(csv_reader, columns.clone())?;
                                let mut rows = vec![];
                                while let Some(row) = rows_stream.next().await {
                                    if let Some(row) = row? {
                                        rows.push(row)
                                    }
                                }
                                rows
                            } else {
                                vec![]
                            };
                            inline_tables.push(InlineTable::new(
                                inline_tables.len() as u64 + 1,
                                name,
                                Arc::new(DataFrame::new(columns, rows)),
                            ));
                        }
                    };
                    HttpCommand::Query {
                        query: query.query().unwrap().to_string(),
                        inline_tables,
                        trace_obj: query.trace_obj().map(|q| q.to_string()),
                    }
                }
                cubeshared::codegen::HttpCommand::HttpResultSet => {
                    let result_set = http_message.command_as_http_result_set().unwrap();
                    let mut result_rows = Vec::new();
                    if let Some(rows) = result_set.rows() {
                        for row in rows.iter() {
                            let mut result_row = Vec::new();
                            if let Some(values) = row.values() {
                                for value in values.iter() {
                                    result_row.push(
                                        value
                                            .string_value()
                                            .map(|s| TableValue::String(s.to_string()))
                                            .unwrap_or(TableValue::Null),
                                    );
                                }
                            }
                            result_rows.push(Row::new(result_row));
                        }
                    }
                    let mut result_columns = Vec::new();
                    if let Some(columns) = result_set.columns() {
                        let mut index = 0;
                        for column in columns.iter() {
                            result_columns.push(Column::new(
                                column.to_string(),
                                ColumnType::String,
                                index,
                            ));
                            index += 1;
                        }
                    }
                    HttpCommand::ResultSet {
                        data_frame: Arc::new(DataFrame::new(result_columns, result_rows)),
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
    use crate::config::{init_test_logger, Config};
    use crate::http::{HttpCommand, HttpMessage, HttpServer};
    use crate::metastore::{Column, ColumnType};
    use crate::mysql::MockSqlAuthService;
    use crate::sql::{timestamp_from_string, InlineTable, QueryPlans, SqlQueryContext, SqlService};
    use crate::store::DataFrame;
    use crate::table::{Row, TableValue};
    use crate::CubeError;
    use async_trait::async_trait;
    use cubeshared::codegen::{
        HttpMessageArgs, HttpQuery, HttpQueryArgs, HttpTable, HttpTableArgs,
    };
    use datafusion::cube_ext;
    use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
    use futures_util::{SinkExt, StreamExt};
    use indoc::indoc;
    use log::trace;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
    use url::Url;

    fn build_types<'a: 'ma, 'ma>(
        builder: &'ma mut FlatBufferBuilder<'a>,
        columns: &Vec<Column>,
    ) -> WIPOffset<Vector<'a, ForwardsUOffset<&'a str>>> {
        let str_types = columns
            .iter()
            .map(|c| builder.create_string(&c.get_column_type().to_string()))
            .collect::<Vec<_>>();
        let types_vec = builder.create_vector(str_types.as_slice());
        types_vec
    }

    #[tokio::test]
    async fn query_test() {
        let message = HttpMessage {
            message_id: 1234,
            command: HttpCommand::Query {
                query: "test query".to_string(),
                inline_tables: vec![],
                trace_obj: Some("test trace".to_string()),
            },
            connection_id: Some("foo".to_string()),
        };
        let bytes = message.bytes();
        let output_message = HttpMessage::read(bytes).await.unwrap();
        assert_eq!(message, output_message);
    }

    #[tokio::test]
    async fn inline_tables_query_test() {
        let columns = vec![
            Column::new("A".to_string(), ColumnType::Int, 0),
            Column::new("B".to_string(), ColumnType::String, 1),
            Column::new("C".to_string(), ColumnType::Timestamp, 2),
        ];
        let rows = vec![
            Row::new(vec![
                TableValue::Int(1),
                TableValue::String("one".to_string()),
                TableValue::Timestamp(timestamp_from_string("2020-01-01T00:00:00.000Z").unwrap()),
            ]),
            Row::new(vec![
                TableValue::Null,
                TableValue::String("two".to_string()),
                TableValue::Timestamp(timestamp_from_string("2020-01-02T00:00:00.000Z").unwrap()),
            ]),
            Row::new(vec![
                TableValue::Int(3),
                TableValue::Null,
                TableValue::Timestamp(timestamp_from_string("2020-01-03T00:00:00.000Z").unwrap()),
            ]),
            Row::new(vec![
                TableValue::Int(4),
                TableValue::String("four".to_string()),
                TableValue::Null,
            ]),
        ];
        let csv_rows = indoc! {"
            1,one,2020-01-01T00:00:00.000Z
            ,two,2020-01-02T00:00:00.000Z
            3,,2020-01-03T00:00:00.000Z
            4,four,
        "};
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
        let query_offset = builder.create_string("query");
        let mut inline_tables_offsets = Vec::with_capacity(1);
        let name_offset = builder.create_string("table");
        let columns_vec = HttpMessage::build_columns(&mut builder, &columns);
        let types_vec = build_types(&mut builder, &columns);
        let csv_rows_value = builder.create_string(csv_rows);
        let connection_id_offset = builder.create_string("foo");
        let inline_table_offset = HttpTable::create(
            &mut builder,
            &HttpTableArgs {
                name: Some(name_offset),
                columns: Some(columns_vec),
                types: Some(types_vec),
                csv_rows: Some(csv_rows_value),
            },
        );
        inline_tables_offsets.push(inline_table_offset);
        let inline_tables_offset = builder.create_vector(inline_tables_offsets.as_slice());
        let query_value = HttpQuery::create(
            &mut builder,
            &HttpQueryArgs {
                query: Some(query_offset),
                inline_tables: Some(inline_tables_offset),
                trace_obj: None,
            },
        );
        let args = HttpMessageArgs {
            message_id: 1234,
            command_type: cubeshared::codegen::HttpCommand::HttpQuery,
            command: Some(query_value.as_union_value()),
            connection_id: Some(connection_id_offset),
        };
        let message = cubeshared::codegen::HttpMessage::create(&mut builder, &args);
        builder.finish(message, None);
        let bytes = builder.finished_data().to_vec();
        let message = HttpMessage::read(bytes).await.unwrap();
        assert_eq!(
            message,
            HttpMessage {
                message_id: 1234,
                command: HttpCommand::Query {
                    query: "query".to_string(),
                    inline_tables: vec![InlineTable::new(
                        1,
                        "table".to_string(),
                        Arc::new(DataFrame::new(columns, rows.clone()))
                    )],
                    trace_obj: None
                },
                connection_id: Some("foo".to_string()),
            }
        );
    }

    pub struct SqlServiceMock {
        message_counter: AtomicU64,
    }

    crate::di_service!(SqlServiceMock, [SqlService]);

    #[async_trait]
    impl SqlService for SqlServiceMock {
        async fn exec_query(&self, _query: &str) -> Result<Arc<DataFrame>, CubeError> {
            todo!()
        }

        async fn exec_query_with_context(
            &self,
            _context: SqlQueryContext,
            query: &str,
        ) -> Result<Arc<DataFrame>, CubeError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let counter = self.message_counter.fetch_add(1, Ordering::Relaxed);
            if query == "close_connection" {
                Err(CubeError::wrong_connection("wrong connection".to_string()))
            } else if query == "error" {
                Err(CubeError::internal("error".to_string()))
            } else {
                Ok(Arc::new(DataFrame::new(
                    vec![Column::new("foo".to_string(), ColumnType::String, 0)],
                    vec![Row::new(vec![TableValue::String(format!("{}", counter))])],
                )))
            }
        }

        async fn plan_query(&self, _query: &str) -> Result<QueryPlans, CubeError> {
            todo!()
        }

        async fn plan_query_with_context(
            &self,
            _context: SqlQueryContext,
            _query: &str,
        ) -> Result<QueryPlans, CubeError> {
            todo!()
        }

        async fn upload_temp_file(
            &self,
            _context: SqlQueryContext,
            _name: String,
            _file_path: &Path,
        ) -> Result<(), CubeError> {
            todo!()
        }

        async fn temp_uploads_dir(&self, _context: SqlQueryContext) -> Result<String, CubeError> {
            todo!()
        }
    }

    #[tokio::test]
    async fn ws_test() {
        init_test_logger().await;

        let sql_service = SqlServiceMock {
            message_counter: AtomicU64::new(0),
        };
        let mut auth = MockSqlAuthService::new();
        auth.expect_authenticate().return_const(Ok(None));

        let config = Config::test("ws_test").config_obj();

        let http_server = Arc::new(HttpServer::new(
            "127.0.0.1:53031".to_string(),
            Arc::new(auth),
            Arc::new(sql_service),
            Duration::from_millis(100),
            Duration::from_millis(10000),
            Duration::from_millis(1000),
            config.transport_max_message_size(),
            config.transport_max_frame_size(),
        ));
        {
            let http_server = http_server.clone();
            cube_ext::spawn(async move { http_server.run_server().await });
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        async fn connect() -> WebSocketStream<MaybeTlsStream<TcpStream>> {
            let (socket, _) = connect_async(Url::parse("ws://127.0.0.1:53031/ws").unwrap())
                .await
                .unwrap();
            socket
        }

        async fn send_query(
            socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
            message_id: u32,
            connection_id: Option<String>,
            query: &str,
        ) {
            socket
                .send(Message::binary(
                    HttpMessage {
                        message_id,
                        command: HttpCommand::Query {
                            query: query.to_string(),
                            inline_tables: vec![],
                            trace_obj: None,
                        },
                        connection_id,
                    }
                    .bytes(),
                ))
                .await
                .unwrap();
        }

        async fn connect_and_send_query(
            message_id: u32,
            connection_id: Option<String>,
            query: &str,
        ) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
            let mut socket = connect().await;
            send_query(&mut socket, message_id, connection_id, query).await;
            socket
        }

        async fn connect_and_send(
            message_id: u32,
            connection_id: Option<String>,
        ) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
            connect_and_send_query(message_id, connection_id, "foo").await
        }

        async fn assert_message(
            socket: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
            counter: &str,
        ) {
            let msg = socket.next().await.unwrap().unwrap();
            let message = HttpMessage::read(msg.into_data()).await.unwrap();
            if let HttpCommand::ResultSet { data_frame } = message.command {
                if let TableValue::String(v) = data_frame
                    .get_rows()
                    .iter()
                    .next()
                    .unwrap()
                    .values()
                    .iter()
                    .next()
                    .unwrap()
                {
                    trace!("Message: {}", v.as_str());
                    assert_eq!(v.as_str(), counter);
                } else {
                    panic!("String expected");
                }
            } else {
                panic!("Result set expected");
            }
        }

        tokio::join!(
            // Two sockets for the same message
            async move {
                let mut socket = connect_and_send(1, Some("foo".to_string())).await;
                assert_message(&mut socket, "0").await;
                socket.close(None).await.unwrap();
            },
            async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let mut socket = connect_and_send(1, Some("foo".to_string())).await;
                assert_message(&mut socket, "0").await;
                socket.close(None).await.unwrap();
            },
            // Orphaned complete message
            async move {
                // takes message 1
                tokio::time::sleep(Duration::from_millis(300)).await;
                let mut socket = connect_and_send(1, Some("bar".to_string())).await;
                socket.close(None).await.unwrap();
            },
            async move {
                tokio::time::sleep(Duration::from_millis(4000)).await;
                let mut socket = connect_and_send(1, Some("bar".to_string())).await;
                assert_message(&mut socket, "5").await;
                socket.close(None).await.unwrap();
            },
            // Retrieve complete message
            async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                // takes message 2
                let mut socket = connect_and_send(2, Some("foo".to_string())).await;
                socket.close(None).await.unwrap();
            },
            async move {
                tokio::time::sleep(Duration::from_millis(3000)).await;
                let mut socket = connect_and_send(2, Some("foo".to_string())).await;
                assert_message(&mut socket, "2").await;
                socket.close(None).await.unwrap();
            },
            async move {
                tokio::time::sleep(Duration::from_millis(3500)).await;
                let mut socket = connect_and_send(2, Some("foo".to_string())).await;
                assert_message(&mut socket, "4").await;
                socket.close(None).await.unwrap();
            },
            // First message but after resolved
            async move {
                tokio::time::sleep(Duration::from_millis(2500)).await;
                let mut socket = connect_and_send(1, Some("foo".to_string())).await;
                assert_message(&mut socket, "3").await;
                socket.close(None).await.unwrap();
            },
        );

        tokio::time::sleep(Duration::from_millis(2500)).await;
        let mut socket = connect_and_send(3, Some("foo".to_string())).await;
        assert_message(&mut socket, "6").await;

        let mut socket2 = connect_and_send(3, Some("foo2".to_string())).await;
        assert_message(&mut socket2, "7").await;

        send_query(&mut socket, 3, Some("foo".to_string()), "close_connection").await;
        socket.next().await.unwrap().unwrap();

        send_query(&mut socket2, 3, Some("foo".to_string()), "error").await;
        socket2.next().await.unwrap().unwrap();

        send_query(&mut socket, 3, Some("foo".to_string()), "foo").await;
        assert!(socket.next().await.unwrap().is_err());

        let mut socket2 = connect_and_send(3, Some("foo2".to_string())).await;
        assert_message(&mut socket2, "10").await;

        http_server.stop_processing().await;
    }
}
