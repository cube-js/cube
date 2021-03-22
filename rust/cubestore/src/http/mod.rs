use std::sync::Arc;

use warp::{Filter, Rejection};

use crate::codegen::http_message_generated::{
    get_root_as_http_message, HttpColumnValue, HttpColumnValueArgs, HttpError, HttpErrorArgs,
    HttpMessageArgs, HttpQuery, HttpQueryArgs, HttpResultSet, HttpResultSetArgs, HttpRow,
    HttpRowArgs,
};
use crate::mysql::SqlAuthService;
use crate::sql::{SqlQueryContext, SqlService};
use crate::store::DataFrame;
use crate::table::TableValue;
use crate::util::WorkerLoop;
use crate::CubeError;
use futures::{SinkExt, StreamExt};
use hex::ToHex;
use http_auth_basic::Credentials;
use log::error;
use log::info;
use log::trace;
use std::net::SocketAddr;
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
pub enum WsError {
    NotAuthorized,
}

impl Reject for WsError {}

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
                        Ok(user) => Ok(SqlQueryContext { user }),
                        Err(_) => Err(warp::reject::custom(WsError::NotAuthorized)),
                    }
                }
            });
        let query_route = warp::path!("ws")
            .and(tx_to_move_filter)
            .and(auth_filter)
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
            }).recover(|err: Rejection| async move {
                if let Some(ws_error) = err.find::<WsError>() {
                    match ws_error {
                        WsError::NotAuthorized => Ok(warp::reply::with_status("Not authorized", StatusCode::FORBIDDEN))
                    }
                } else {
                    Err(err)
                }

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
                tokio::spawn(async move {
                    let res =
                        HttpServer::process_command(sql_service, sql_query_context, command).await;
                    let message = match res {
                        Ok(command) => HttpMessage {
                            message_id,
                            command,
                        },
                        Err(e) => HttpMessage {
                            message_id,
                            command: HttpCommand::Error {
                                error: e.to_string(),
                            },
                        },
                    };
                    if let Err(e) = sender.send(message).await {
                        error!("Send result channel error: {:?}", e);
                    }
                });
                Ok(())
            },
        );
        let cancel_token = self.cancel_token.clone();
        let (_, server_future) = warp::serve(
            query_route, // .or(import_route)
        )
        .bind_with_graceful_shutdown(addr, async move { cancel_token.cancelled().await });
        let _ = tokio::join!(process_loop, server_future);

        Ok(())
    }

    pub async fn process_command(
        sql_service: Arc<dyn SqlService>,
        sql_query_context: SqlQueryContext,
        command: HttpCommand,
    ) -> Result<HttpCommand, CubeError> {
        match command {
            HttpCommand::Query { query } => Ok(HttpCommand::ResultSet {
                data_frame: sql_service
                    .exec_query_with_context(sql_query_context, &query)
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

#[derive(Debug)]
pub struct HttpMessage {
    message_id: u32,
    command: HttpCommand,
}

#[derive(Debug)]
pub enum HttpCommand {
    Query { query: String },
    ResultSet { data_frame: DataFrame },
    Error { error: String },
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
                HttpCommand::Query { query } => {
                    let query_offset = builder.create_string(&query);
                    Some(
                        HttpQuery::create(
                            &mut builder,
                            &HttpQueryArgs {
                                query: Some(query_offset),
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
                    let columns = data_frame
                        .get_columns()
                        .iter()
                        .map(|c| c.get_name().as_str())
                        .collect::<Vec<_>>();
                    let columns_vec = builder.create_vector_of_strings(columns.as_slice());

                    let mut row_offsets = Vec::with_capacity(data_frame.get_rows().len());
                    for row in data_frame.get_rows().iter() {
                        let mut value_offsets = Vec::with_capacity(row.values().len());
                        for value in row.values().iter() {
                            let value = match value {
                                TableValue::Null => HttpColumnValue::create(
                                    &mut builder,
                                    &HttpColumnValueArgs { string_value: None },
                                ),
                                TableValue::String(v) => {
                                    let string_value = Some(builder.create_string(v));
                                    HttpColumnValue::create(
                                        &mut builder,
                                        &HttpColumnValueArgs { string_value },
                                    )
                                }
                                TableValue::Int(v) => {
                                    let string_value = Some(builder.create_string(&v.to_string()));
                                    HttpColumnValue::create(
                                        &mut builder,
                                        &HttpColumnValueArgs { string_value },
                                    )
                                }
                                TableValue::Decimal(v) => {
                                    let string_value = Some(builder.create_string(&v.to_string()));
                                    HttpColumnValue::create(
                                        &mut builder,
                                        &HttpColumnValueArgs { string_value },
                                    )
                                }
                                TableValue::Float(v) => {
                                    let string_value = Some(builder.create_string(&v.to_string()));
                                    HttpColumnValue::create(
                                        &mut builder,
                                        &HttpColumnValueArgs { string_value },
                                    )
                                }
                                TableValue::Bytes(v) => {
                                    let string_value = Some(builder.create_string(&format!(
                                        "0x{}",
                                        v.encode_hex_upper::<String>()
                                    )));
                                    HttpColumnValue::create(
                                        &mut builder,
                                        &HttpColumnValueArgs { string_value },
                                    )
                                }
                                TableValue::Timestamp(v) => {
                                    let string_value = Some(builder.create_string(&v.to_string()));
                                    HttpColumnValue::create(
                                        &mut builder,
                                        &HttpColumnValueArgs { string_value },
                                    )
                                }
                                TableValue::Boolean(v) => {
                                    let string_value = Some(builder.create_string(&v.to_string()));
                                    HttpColumnValue::create(
                                        &mut builder,
                                        &HttpColumnValueArgs { string_value },
                                    )
                                }
                            };
                            value_offsets.push(value);
                        }
                        let values = Some(builder.create_vector(value_offsets.as_slice()));
                        let row = HttpRow::create(&mut builder, &HttpRowArgs { values });
                        row_offsets.push(row);
                    }

                    let rows = Some(builder.create_vector(row_offsets.as_slice()));

                    Some(
                        HttpResultSet::create(
                            &mut builder,
                            &HttpResultSetArgs {
                                columns: Some(columns_vec),
                                rows,
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

    pub fn read(buffer: Vec<u8>) -> Result<Self, CubeError> {
        let http_message = get_root_as_http_message(buffer.as_slice());
        Ok(HttpMessage {
            message_id: http_message.message_id(),
            command: match http_message.command_type() {
                crate::codegen::http_message_generated::HttpCommand::HttpQuery => {
                    let query = http_message.command_as_http_query().unwrap();
                    HttpCommand::Query {
                        query: query.query().unwrap().to_string(),
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
