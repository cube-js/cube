pub mod status;

use std::sync::Arc;

use warp::{Filter, Rejection, Reply};

use crate::cachestore::QUEUE_ITEM_PROCESS_ID_MAX_LEN;
use crate::metastore::{Column, ColumnType, ImportFormat};
use crate::mysql::SqlAuthService;
use crate::sql::{
    InlineTable, InlineTables, QueryParameter, QueryParameters, SqlQueryContext, SqlService,
};
use crate::store::DataFrame;
use crate::table::{Row, TableValue};
use crate::util::WorkerLoop;
use crate::{app_metrics, CubeError};
use cubeshared::codegen::{
    root_as_http_message, HttpColumnValue, HttpColumnValueArgs, HttpError, HttpErrorArgs,
    HttpMessageArgs, HttpParameterValue, HttpQuery, HttpQueryArgs, HttpQueryResult,
    HttpQueryResultArgs, HttpQueryResultArrow, HttpQueryResultArrowArgs, HttpQueryResultCompleted,
    HttpQueryResultCompletedArgs, HttpQueryResultData, HttpResultSet, HttpResultSetArgs, HttpRow,
    HttpRowArgs, QueryResultFormat,
};
use cubeshared::flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use datafusion::cube_ext;
use futures::{SinkExt, Stream, StreamExt};
use futures_timer::Delay;
use hex::ToHex;
use http_auth_basic::Credentials;
use log::error;
use log::info;
use log::trace;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::error::Error as StdError;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite;
use tokio_util::sync::CancellationToken;
use warp::filters::ws::{Message, Ws};
use warp::http::StatusCode;
use warp::reject::Reject;

/// Close code the WebSocket protocol reserves for a message a peer refuses to
/// process because it is too large (RFC 6455 section 7.4.1, "Message Too Big").
const MESSAGE_TOO_BIG_CLOSE_CODE: u16 = 1009;

/// Close code the WebSocket protocol reserves for a peer that should come back
/// later (RFC 6455 section 7.4.1, "Try Again Later"). An evicted connection was
/// recycled to make room for a newer one, not broken, and a client that still
/// needs it should reconnect rather than report a transport failure.
const CONNECTION_EVICTED_CLOSE_CODE: u16 = 1013;

/// How long the server waits for an evicted connection's close frame to go out.
///
/// The connection's entry leaves the counter when it is evicted, not when its
/// task ends, so this is also how long its descriptor can outlive the slot it
/// accounted for: the steady-state overshoot is the eviction rate times this
/// bound. Sending is what can block — a peer whose receive window is closed
/// keeps the frame in our buffer indefinitely — and a close frame is a handful
/// of bytes, so a peer that cannot take them within this long is not going to.
const EVICTED_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a caller waits for the connection counter's lock before giving up.
///
/// Nothing awaits inside the critical section — it is a map lookup and an
/// insert — so waiting even this long is not reachable by the code as written;
/// the bound is here so that no reasoning about the callers is needed to know
/// this cannot stall a connection. It is not shorter because a thread holding
/// the lock can be descheduled for milliseconds under CPU pressure, and giving
/// up then would quietly stop counting connections exactly when the cap matters
/// most. Both timeouts log, so a bound that is ever reached is visible.
const COUNTER_LOCK_TIMEOUT: Duration = Duration::from_millis(100);

/// How much room the transport is given above the configured sizes.
///
/// The size limit is enforced here rather than by the transport, so that an
/// over-limit request can be answered with an error naming the message it
/// belongs to, on a connection that stays up for everything else multiplexed
/// over it. That needs the message to arrive whole: `tungstenite` raises its
/// capacity error as soon as the frame header is parsed, before the payload is
/// read, which leaves the frame stream desynchronized and the message id
/// unread. So the transport is configured a factor above the limit, and only
/// enforces it as a backstop against a peer that would otherwise make the
/// server buffer without bound. A message in between is what gets the readable
/// error; past the backstop there is nothing to answer with but a close frame.
///
/// The frame limit is given the same headroom, and has to be: a client sends a
/// query as a single frame, so an exact frame limit would refuse an over-limit
/// message at the transport before the handler ever saw it, and at the default
/// configuration — where `CUBESTORE_TRANSPORT_MAX_FRAME_SIZE` and
/// `CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE` are both `64 << 20` — that is every
/// over-limit message, leaving the readable error unreachable.
///
/// Frame size is not re-checked in the handler, because reassembly happens
/// below `warp` and individual frames are never visible up here. So with the
/// two knobs configured apart, `CUBESTORE_TRANSPORT_MAX_FRAME_SIZE` bounds a
/// single allocation at the headroom multiple of its configured value, and the
/// message limit is what actually enforces the policy.
const TRANSPORT_SIZE_HEADROOM: usize = 2;

/// Recognizes the error raised when an incoming message exceeds
/// `CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE` or `CUBESTORE_TRANSPORT_MAX_FRAME_SIZE`,
/// and renders it as a close reason. `warp` boxes the underlying `tungstenite`
/// error, so it has to be recovered through `source()`.
fn message_too_large_reason(
    e: &warp::Error,
    max_message_size: usize,
    max_frame_size: usize,
) -> Option<String> {
    match e.source()?.downcast_ref::<tungstenite::Error>()? {
        tungstenite::Error::Capacity(tungstenite::error::CapacityError::MessageTooLong {
            size,
            max_size,
        }) => {
            // `max_size` is the backstop the transport was configured with,
            // which is the configured limit times the headroom. Report what
            // the operator actually set, since that is the number they can
            // change.
            let configured = max_size / TRANSPORT_SIZE_HEADROOM;
            // The same error covers both caps and doesn't say which one
            // fired, so the value has to name the knob: a frame limit
            // configured below the message limit would otherwise be reported
            // as a message limit that never refused anything, sending an
            // operator to the wrong environment variable. When the two are
            // equal, as they are by default, the number is the same either
            // way and the message limit is the one to name.
            let limit = if configured == max_message_size {
                "message"
            } else if configured == max_frame_size {
                "frame"
            } else {
                "transport"
            };
            Some(format!(
                "Message of {} bytes exceeds the maximum {} size of {} bytes",
                size, limit, configured
            ))
        }
        _ => None,
    }
}

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
    ws_connections: Arc<WsConnectionCounter>,
}

crate::di_service!(HttpServer, []);

/// Caps how many concurrent websocket connections one authenticated user may
/// hold. A client that leaks connections otherwise exhausts the process file
/// descriptor table for every other user sharing the node.
///
/// At the limit the user's oldest connection is closed to admit the new one,
/// rather than the new one being refused: a client that still needs the closed
/// connection reconnects, while one that had forgotten about it simply loses it.
/// A limit of 0 disables the cap.
pub struct WsConnectionCounter {
    limit: usize,
    /// Live connections per user, keyed by a monotonic id so the first entry is the
    /// oldest, holding the token that asks the connection task to close.
    ///
    /// Synchronous on purpose, not a `tokio` lock behind `util::lock::acquire_lock`:
    /// entries are removed from `WsConnectionGuard::drop`, and a destructor cannot
    /// `.await`. Releasing anywhere else would leak the slot on a panic, a cancelled
    /// task, or a connection that dies before the upgrade completes. Nothing awaits
    /// inside the critical section either — `acquire` is not an `async fn`, so one
    /// cannot be added — which is the shape the `tokio::sync::Mutex` docs point at a
    /// standard-library lock for. Every caller still takes it with a bound, so the
    /// property does not rest on that reasoning holding: see COUNTER_LOCK_TIMEOUT.
    users: parking_lot::Mutex<HashMap<String, BTreeMap<u64, CancellationToken>>>,
    next_id: AtomicU64,
}

/// `None` when the lock could not be taken within [`COUNTER_LOCK_TIMEOUT`].
/// Callers fail open: the cap is a safety net, so a counter that is briefly
/// unavailable must not be what refuses or stalls a connection.
fn try_lock_users(
    users: &parking_lot::Mutex<HashMap<String, BTreeMap<u64, CancellationToken>>>,
) -> Option<parking_lot::MutexGuard<'_, HashMap<String, BTreeMap<u64, CancellationToken>>>> {
    users.try_lock_for(COUNTER_LOCK_TIMEOUT)
}

/// Frees the slot taken by [`WsConnectionCounter::acquire`] when the connection
/// task ends, however it ends, and carries the token that task waits on.
pub struct WsConnectionGuard {
    counter: Arc<WsConnectionCounter>,
    user: Option<String>,
    id: u64,
    cancel: CancellationToken,
}

impl WsConnectionCounter {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            users: parking_lot::Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        })
    }

    /// Unauthenticated connections and a disabled cap stay untracked.
    pub fn acquire(self: &Arc<Self>, user: Option<&str>) -> WsConnectionGuard {
        let cancel = CancellationToken::new();
        let user = match user {
            Some(user) if self.limit > 0 => user,
            _ => {
                return WsConnectionGuard {
                    counter: self.clone(),
                    user: None,
                    id: 0,
                    cancel,
                }
            }
        };

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let evicted = {
            let mut users = match try_lock_users(&self.users) {
                Some(users) => users,
                None => {
                    log::error!(
                        "Timed out locking the websocket connection counter; admitting an untracked connection (user: {})",
                        user,
                    );
                    return WsConnectionGuard {
                        counter: self.clone(),
                        user: None,
                        id: 0,
                        cancel,
                    };
                }
            };
            let entries = users.entry(user.to_string()).or_default();
            let evicted = if entries.len() >= self.limit {
                let oldest = *entries.keys().next().expect("a full map has an entry");
                // Removed here rather than left to the victim's own `drop`, which
                // runs later: otherwise this admission would overshoot the limit.
                entries.remove(&oldest)
            } else {
                None
            };
            entries.insert(id, cancel.clone());
            evicted
        };

        // Outside the lock. Cancelling only wakes the victim's task, so this cannot
        // deadlock, but there is no reason to hold the lock across it.
        if let Some(evicted) = evicted {
            evicted.cancel();
        }

        WsConnectionGuard {
            counter: self.clone(),
            user: Some(user.to_string()),
            id,
            cancel,
        }
    }

    pub fn count(&self, user: &str) -> usize {
        try_lock_users(&self.users)
            .and_then(|users| users.get(user).map(BTreeMap::len))
            .unwrap_or(0)
    }
}

impl WsConnectionGuard {
    /// Resolves when this connection has been chosen to make room for a newer one.
    pub async fn evicted(&self) {
        self.cancel.cancelled().await
    }
}

impl Drop for WsConnectionGuard {
    fn drop(&mut self) {
        let user = match &self.user {
            Some(user) => user,
            None => return,
        };
        let mut users = match try_lock_users(&self.counter.users) {
            Some(users) => users,
            None => {
                // The entry stays behind, but it is the oldest one of that user by
                // construction, so the next admission evicts it: cancelling an
                // already finished connection's token is a no-op.
                log::error!(
                    "Timed out locking the websocket connection counter; leaving a finished connection's slot to be evicted (user: {})",
                    user,
                );
                return;
            }
        };
        if let Some(entries) = users.get_mut(user) {
            // Idempotent: an evicting `acquire` may have removed this entry already.
            entries.remove(&self.id);
            // Dropped when empty so a churn of one-off users can't grow the map.
            if entries.is_empty() {
                users.remove(user);
            }
        }
    }
}

#[derive(Debug)]
pub enum CubeRejection {
    NotAuthorized,
    BadRequest(String),
    Internal(String),
}

impl From<CubeError> for warp::reject::Rejection {
    fn from(e: CubeError) -> Self {
        warp::reject::custom(CubeRejection::Internal(e.message.to_string()))
    }
}

/// Rate limit errors are expected under load and are still returned to the client,
/// so they shouldn't pollute the error log.
fn is_rate_limit_error(e: &CubeError) -> bool {
    e.message.to_ascii_lowercase().contains("rate limit")
}

/// `process_id` is connection scoped and reaches the cachestore as `QueueItem.process_id`,
/// so it's bounded here, on its only ingress.
fn check_process_id_header(process_id: &Option<String>) -> Result<(), CubeRejection> {
    if let Some(id) = process_id {
        if id.len() > QUEUE_ITEM_PROCESS_ID_MAX_LEN {
            return Err(CubeRejection::BadRequest(format!(
                "x-process-id header exceeds maximum allowed length of {} characters",
                QUEUE_ITEM_PROCESS_ID_MAX_LEN
            )));
        }
    }

    Ok(())
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
        max_ws_connections_per_user: usize,
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
            ws_connections: WsConnectionCounter::new(max_ws_connections_per_user),
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
            .and(warp::header::optional("x-process-id"))
            .and_then(
                move |auth_header: Option<String>, process_id: Option<String>| {
                    let auth_service = auth_service.clone();
                    async move {
                        check_process_id_header(&process_id).map_err(warp::reject::custom)?;

                        let res = HttpServer::authorize(auth_service, auth_header).await;
                        match res {
                            Ok(user) => Ok(SqlQueryContext {
                                user,
                                inline_tables: InlineTables::new(),
                                parameters: None,
                                trace_obj: None,
                                process_id,
                            }),
                            Err(_) => Err(warp::reject::custom(CubeRejection::NotAuthorized)),
                        }
                    }
                },
            );

        let context_filter = tx_to_move_filter.and(auth_filter.clone());

        let context_filter_to_move = context_filter.clone();
        let max_frame_size = self.max_frame_size.clone();
        let max_message_size = self.max_message_size.clone();
        let ws_connections = self.ws_connections.clone();
        let ws_connections_filter = warp::any().map(move || ws_connections.clone());

        let query_route = warp::path!("ws")
            .and(context_filter_to_move)
            .and(ws_connections_filter)
            .and(warp::ws::ws())
            .and_then(move |tx: mpsc::Sender<(mpsc::Sender<Arc<HttpMessage>>, SqlQueryContext, HttpMessage)>, sql_query_context: SqlQueryContext, ws_connections: Arc<WsConnectionCounter>, ws: Ws| async move {
                let tx_to_move = tx.clone();
                let sql_query_context = sql_query_context.clone();
                let connection_guard = ws_connections.acquire(sql_query_context.user.as_deref());
                let reply = ws.max_frame_size(max_frame_size.saturating_mul(TRANSPORT_SIZE_HEADROOM)).max_message_size(max_message_size.saturating_mul(TRANSPORT_SIZE_HEADROOM)).on_upgrade(async move |mut web_socket| {
                    // Lives as long as the connection task; dropping it frees the slot.
                    let connection_guard = connection_guard;
                    let process_id = sql_query_context.process_id.as_deref().unwrap_or("None");
                    trace!("WebSocket connection established (process_id: {})", process_id);
                    let (response_tx, mut response_rx) = mpsc::channel::<Arc<HttpMessage>>(10000);
                    loop {
                        tokio::select! {
                            _ = connection_guard.evicted() => {
                                log::warn!(
                                    "Closing websocket connection to admit a newer one for the same user (process_id: {})",
                                    process_id,
                                );
                                // A close frame naming the reason rather than a bare drop,
                                // so the client can tell being recycled from a network
                                // blip. Bounded, because the descriptor is already
                                // unaccounted for: see EVICTED_CLOSE_TIMEOUT.
                                let close = web_socket.send(Message::close_with(
                                    CONNECTION_EVICTED_CLOSE_CODE,
                                    "connection evicted to admit a newer one for the same user",
                                ));
                                match tokio::time::timeout(EVICTED_CLOSE_TIMEOUT, close).await {
                                    Ok(Ok(())) => {}
                                    Ok(Err(e)) => error!("Websocket close send error: {:?}", e),
                                    Err(_) => log::warn!(
                                        "Timed out sending the close frame of an evicted websocket connection"
                                    ),
                                }
                                break;
                            }
                            Some(res) = response_rx.recv() => {
                                trace!("Sending web socket response (process_id: {})", process_id);
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
                                        // Past the transport backstop the payload is refused
                                        // before it is read, so the frame stream is left
                                        // desynchronized and the message id is never seen: the
                                        // connection can't be reused and the error can't be
                                        // attributed to a query the way an over-limit request
                                        // under the backstop is. Answer with the close code
                                        // reserved for this instead of dropping the connection
                                        // silently, so the client can report the size rather than
                                        // a bare disconnect it would otherwise retry.
                                        match message_too_large_reason(&e, max_message_size, max_frame_size) {
                                            Some(reason) => {
                                                error!("Websocket message too large: {}", reason);
                                                let send_res = web_socket.send(
                                                    Message::close_with(MESSAGE_TOO_BIG_CLOSE_CODE, reason)
                                                ).await;
                                                if let Err(e) = send_res {
                                                    error!("Websocket close send error: {:?}", e)
                                                }
                                            }
                                            None => error!("Websocket error: {:?}", e),
                                        }
                                        break;
                                    }
                                    Ok(msg) => {
                                        if msg.is_binary() {
                                            let message_buffer = msg.into_bytes();
                                            let http_message = match root_as_http_message(&message_buffer) {
                                                Err(e) => {
                                                    error!("Websocket message deserialization error: {:?}", e);
                                                    continue;
                                                },
                                                Ok(http_message) => http_message,
                                            };

                                            let message_id = http_message.message_id();
                                            let connection_id = http_message.connection_id().map(|s| s.to_string());

                                            // Refused here rather than by the transport so the
                                            // answer can name the message it belongs to and the
                                            // connection survives for everything else in flight
                                            // on it. See TRANSPORT_SIZE_HEADROOM.
                                            if message_buffer.len() > max_message_size {
                                                let error = format!(
                                                    "Request of {} bytes exceeds the maximum message size of {} bytes. Reduce the size of the query, e.g. by sending fewer or smaller inline tables, or raise CUBESTORE_TRANSPORT_MAX_MESSAGE_SIZE.",
                                                    message_buffer.len(), max_message_size
                                                );
                                                error!("Websocket message too large: {}", error);
                                                let send_res = web_socket.send(
                                                    Message::binary(HttpMessage { message_id, connection_id, command: HttpCommand::Error { error } }.bytes())
                                                ).await;
                                                if let Err(e) = send_res {
                                                    error!("Websocket message send error: {:?}", e)
                                                }
                                                continue;
                                            }

                                            match HttpMessage::read(http_message).await {
                                                Err(e) => {
                                                    error!("Websocket message read error: {:?}", e);

                                                    let send_res = web_socket.send(
                                                        Message::binary(HttpMessage { message_id, connection_id, command: HttpCommand::Error { error: e.to_string() } }.bytes())
                                                    ).await;
                                                    if let Err(e) = send_res {
                                                        error!("Websocket message send error: {:?}", e)
                                                    }
                                                    break;
                                                },
                                                Ok(msg) => {
                                                    trace!("Received web socket message (process_id: {})", process_id);
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
                });
                Result::<_, Rejection>::Ok(warp::reply::with_header(reply, "X-CubeStore-Version", env!("CARGO_PKG_VERSION")))
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
                                let command_text = match &command {
                                    HttpCommand::Query { query, .. } => format!("HttpCommand::Query {{ query: {:?} }}", query),
                                    HttpCommand::Error { error } => format!("HttpCommand::Error {{ error: {:?} }}", error),
                                    HttpCommand::CloseConnection { error } => format!("HttpCommand::CloseConnection {{ error: {:?} }}", error),
                                    HttpCommand::ResultSet { .. } => format!("HttpCommand::ResultSet {{}}"),
                                    HttpCommand::QueryResultArrow { .. } => format!("HttpCommand::QueryResultArrow {{}}"),
                                    HttpCommand::QueryResultCompleted => format!("HttpCommand::QueryResultCompleted"),
                                };
                                let level = if is_rate_limit_error(&e) {
                                    log::Level::Warn
                                } else {
                                    log::Level::Error
                                };
                                log::log!(
                                    level,
                                    "Error processing HTTP command (connection_id={}): {}\nThe command: {}",
                                    if let Some(c) = connection_id.as_ref() { c.as_str() } else { "(None)" },
                                    e.display_with_backtrace(),
                                    command_text,
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
                        let command_text = match &command {
                            HttpCommand::Query { query, .. } => format!("HttpCommand::Query {{ query: {:?} }}", query),
                            HttpCommand::Error { error } => format!("HttpCommand::Error {{ error: {:?} }}", error),
                            HttpCommand::CloseConnection { error } => format!("HttpCommand::CloseConnection {{ error: {:?} }}", error),
                            HttpCommand::ResultSet { .. } => format!("HttpCommand::ResultSet {{}}"),
                            HttpCommand::QueryResultArrow { .. } => format!("HttpCommand::QueryResultArrow {{}}"),
                            HttpCommand::QueryResultCompleted => format!("HttpCommand::QueryResultCompleted"),
                        };
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
                                let level = if is_rate_limit_error(&e) {
                                    log::Level::Warn
                                } else {
                                    log::Level::Error
                                };
                                log::log!(
                                    level,
                                    "Error processing HTTP command: {}\nThe command: {}",
                                    e.display_with_backtrace(),
                                    command_text,
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
                        CubeRejection::BadRequest(e) => {
                            obj.insert("error".to_string(), e.to_string());
                            Ok(warp::reply::with_status(
                                warp::reply::json(&obj),
                                StatusCode::BAD_REQUEST,
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
                parameters,
                response_format,
            } => {
                let query_result = sql_service
                    .exec_query_with_context(
                        sql_query_context
                            .with_trace_obj(trace_obj)
                            .with_inline_tables(&inline_tables)
                            .with_parameters(&parameters),
                        &query,
                    )
                    .await?;
                match response_format {
                    QueryResultFormat::Legacy => Ok(HttpCommand::ResultSet {
                        data_frame: query_result.collect().await?,
                    }),
                    QueryResultFormat::Arrow => {
                        // Commands that complete without a result set (CREATE
                        // TABLE/INSERT, queue/cache writes) carry zero columns.
                        // There's no Arrow stream to build for them, so signal
                        // completion with a dedicated result instead.
                        if query_result.schema().fields().is_empty() {
                            Ok(HttpCommand::QueryResultCompleted)
                        } else {
                            let data = query_result.to_arrow_ipc_stream().await?;
                            Ok(HttpCommand::QueryResultArrow { data })
                        }
                    }
                    other => Err(CubeError::user(format!(
                        "Unsupported response_format: {:?}",
                        other
                    ))),
                }
            }
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
        parameters: Option<QueryParameters>,
        response_format: QueryResultFormat,
    },
    ResultSet {
        data_frame: Arc<DataFrame>,
    },
    QueryResultArrow {
        /// Pre-serialized Arrow IPC stream payload. May contain multiple
        /// RecordBatch messages following the schema header; consumers must
        /// decode it with a streaming IPC reader.
        data: Vec<u8>,
    },
    /// Command completed without a result set (zero columns) — e.g. CREATE
    /// TABLE/INSERT or queue/cache writes. Only produced for Arrow-format
    /// requests; the legacy path returns an empty `ResultSet` instead.
    QueryResultCompleted,
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
        let mut data_frame_serialization_start = None::<SystemTime>;
        let args = HttpMessageArgs {
            message_id: self.message_id,
            command_type: match self.command {
                HttpCommand::Query { .. } => cubeshared::codegen::HttpCommand::HttpQuery,
                HttpCommand::ResultSet { .. } => cubeshared::codegen::HttpCommand::HttpResultSet,
                HttpCommand::QueryResultArrow { .. } | HttpCommand::QueryResultCompleted => {
                    cubeshared::codegen::HttpCommand::HttpQueryResult
                }
                HttpCommand::CloseConnection { .. } | HttpCommand::Error { .. } => {
                    cubeshared::codegen::HttpCommand::HttpError
                }
            },
            command: match &self.command {
                HttpCommand::Query {
                    query,
                    inline_tables,
                    trace_obj,
                    parameters,
                    response_format,
                } => {
                    let query_offset = builder.create_string(&query);
                    let trace_obj_offset = trace_obj.as_ref().map(|o| builder.create_string(o));

                    if !inline_tables.is_empty() {
                        panic!("serializing inline_tables is not implemented")
                    }

                    if parameters.is_some() {
                        panic!("serializing parameters is not implemented")
                    }

                    Some(
                        HttpQuery::create(
                            &mut builder,
                            &HttpQueryArgs {
                                query: Some(query_offset),
                                inline_tables: None,
                                trace_obj: trace_obj_offset,
                                parameters: None,
                                response_format: *response_format,
                            },
                        )
                        .as_union_value(),
                    )
                }
                HttpCommand::QueryResultArrow { data } => {
                    let payload = builder.create_vector(data);
                    let arrow_table = HttpQueryResultArrow::create(
                        &mut builder,
                        &HttpQueryResultArrowArgs {
                            data: Some(payload),
                            // We don't support streaming for now, but clients should implement it
                            // according to the protocol specification
                            is_last: true,
                        },
                    );
                    Some(
                        HttpQueryResult::create(
                            &mut builder,
                            &HttpQueryResultArgs {
                                data_type: HttpQueryResultData::HttpQueryResultArrow,
                                data: Some(arrow_table.as_union_value()),
                            },
                        )
                        .as_union_value(),
                    )
                }
                HttpCommand::QueryResultCompleted => {
                    let completed_table = HttpQueryResultCompleted::create(
                        &mut builder,
                        &HttpQueryResultCompletedArgs {},
                    );
                    Some(
                        HttpQueryResult::create(
                            &mut builder,
                            &HttpQueryResultArgs {
                                data_type: HttpQueryResultData::HttpQueryResultCompleted,
                                data: Some(completed_table.as_union_value()),
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
                    data_frame_serialization_start = Some(SystemTime::now());
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
        let result = builder.finished_data().to_vec(); // TODO copy
        if let Some(data_frame_serialization_start) = data_frame_serialization_start {
            app_metrics::HTTP_MESSAGE_DATA_FRAME_SERIALIZATION_TIME_US.report(
                data_frame_serialization_start
                    .elapsed()
                    .unwrap_or_else(|_| Duration::ZERO)
                    .as_micros() as i64,
            );
        }
        result
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

    pub async fn read<'a>(
        http_message: cubeshared::codegen::HttpMessage<'a>,
    ) -> Result<Self, CubeError> {
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

                    let parameters = if let Some(http_params) = query.parameters() {
                        let mut res = Vec::new();

                        for http_param in http_params.iter() {
                            let value = match http_param.value_type() {
                                HttpParameterValue::NullValue => QueryParameter::Null,
                                HttpParameterValue::Int64Value => QueryParameter::Int64Value(
                                    http_param.value_as_int_64_value().unwrap().v(),
                                ),
                                HttpParameterValue::BoolValue => QueryParameter::BoolValue(
                                    http_param.value_as_bool_value().unwrap().v(),
                                ),
                                HttpParameterValue::StringValue => QueryParameter::StringValue(
                                    http_param.value_as_string_value().unwrap().v().to_string(),
                                ),
                                HttpParameterValue::BinaryValue => QueryParameter::BinaryValue(
                                    http_param
                                        .value_as_binary_value()
                                        .unwrap()
                                        .v()
                                        .iter()
                                        .collect::<Vec<u8>>(),
                                ),
                                HttpParameterValue::Float64Value => QueryParameter::Float64Value(
                                    http_param.value_as_float_64_value().unwrap().v(),
                                ),
                                value_type => {
                                    return Err(CubeError::internal(format!(
                                        "Unsupported parameter type: {:?}",
                                        value_type
                                    )))
                                }
                            };

                            res.push(value);
                        }

                        Some(res)
                    } else {
                        None
                    };

                    HttpCommand::Query {
                        query: query.query().unwrap().to_string(),
                        trace_obj: query.trace_obj().map(|q| q.to_string()),
                        inline_tables,
                        parameters,
                        response_format: query.response_format(),
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
    use super::*;
    use crate::config::{init_test_logger, Config};
    use crate::http::{HttpCommand, HttpMessage, HttpServer};
    use crate::metastore::{Column, ColumnType};
    use crate::mysql::MockSqlAuthService;
    use crate::sql::{
        timestamp_from_string, InlineTable, QueryPlans, QueryResult, SqlQueryContext, SqlService,
    };
    use crate::store::DataFrame;
    use crate::table::{Row, TableValue};
    use crate::CubeError;
    use async_trait::async_trait;
    use cubeshared::codegen::{
        HttpMessageArgs, HttpQuery, HttpQueryArgs, HttpTable, HttpTableArgs,
    };
    use cubeshared::flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
    use datafusion::cube_ext;
    use futures_util::{SinkExt, StreamExt};
    use indoc::indoc;
    use log::trace;
    use pretty_assertions::assert_eq;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::handshake::client::Request;
    use tokio_tungstenite::tungstenite::http::HeaderValue;
    use tokio_tungstenite::tungstenite::{Error as WsError, Message};
    use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
    use url::Url;

    /// Minimal SqlService that always replies with a fixed DataFrame, used to
    /// drive process_command in unit tests.
    struct StubService(Arc<DataFrame>);
    crate::di_service!(StubService, [SqlService]);
    #[async_trait]
    impl SqlService for StubService {
        async fn exec_query(&self, _q: &str) -> Result<QueryResult, CubeError> {
            unimplemented!("Mock")
        }
        async fn exec_query_with_context(
            &self,
            _ctx: SqlQueryContext,
            _q: &str,
        ) -> Result<QueryResult, CubeError> {
            Ok(QueryResult::Frame(self.0.clone()))
        }
        async fn plan_query(&self, _q: &str) -> Result<QueryPlans, CubeError> {
            unimplemented!("Mock")
        }
        async fn plan_query_with_context(
            &self,
            _ctx: SqlQueryContext,
            _q: &str,
        ) -> Result<QueryPlans, CubeError> {
            unimplemented!("Mock")
        }
        async fn upload_temp_file(
            &self,
            _ctx: SqlQueryContext,
            _name: String,
            _path: &Path,
        ) -> Result<(), CubeError> {
            unimplemented!("Mock")
        }
        async fn temp_uploads_dir(&self, _ctx: SqlQueryContext) -> Result<String, CubeError> {
            unimplemented!("Mock")
        }
    }

    /// Records what `upload_temp_file` found on disk, so a test can assert the
    /// body had fully landed before the path was handed off.
    struct UploadStubService {
        dir: std::path::PathBuf,
        seen: std::sync::Mutex<Option<Vec<u8>>>,
    }
    crate::di_service!(UploadStubService, [SqlService]);
    #[async_trait]
    impl SqlService for UploadStubService {
        async fn exec_query(&self, _q: &str) -> Result<QueryResult, CubeError> {
            unimplemented!("Mock")
        }
        async fn exec_query_with_context(
            &self,
            _ctx: SqlQueryContext,
            _q: &str,
        ) -> Result<QueryResult, CubeError> {
            unimplemented!("Mock")
        }
        async fn plan_query(&self, _q: &str) -> Result<QueryPlans, CubeError> {
            unimplemented!("Mock")
        }
        async fn plan_query_with_context(
            &self,
            _ctx: SqlQueryContext,
            _q: &str,
        ) -> Result<QueryPlans, CubeError> {
            unimplemented!("Mock")
        }
        async fn upload_temp_file(
            &self,
            _ctx: SqlQueryContext,
            _name: String,
            path: &Path,
        ) -> Result<(), CubeError> {
            *self.seen.lock().unwrap() = Some(std::fs::read(path)?);
            Ok(())
        }
        async fn temp_uploads_dir(&self, _ctx: SqlQueryContext) -> Result<String, CubeError> {
            Ok(self.dir.to_string_lossy().to_string())
        }
    }

    #[test]
    fn acquire_evicts_the_oldest_connection_to_admit_a_new_one() {
        let counter = WsConnectionCounter::new(2);
        let oldest = counter.acquire(Some("tenant-a"));
        let newer = counter.acquire(Some("tenant-a"));
        assert_eq!(counter.count("tenant-a"), 2);

        let newcomer = counter.acquire(Some("tenant-a"));

        assert!(
            oldest.cancel.is_cancelled(),
            "the oldest connection is the one asked to close"
        );
        assert!(!newer.cancel.is_cancelled());
        assert!(!newcomer.cancel.is_cancelled());
        assert_eq!(
            counter.count("tenant-a"),
            2,
            "the victim leaves before the newcomer is inserted, so the limit never overshoots"
        );

        // The victim's own drop runs later and must not take another entry with it,
        // which is why entries are keyed by a monotonic id.
        drop(oldest);
        assert_eq!(counter.count("tenant-a"), 2);

        // Eviction keeps following insertion order.
        let latest = counter.acquire(Some("tenant-a"));
        assert!(
            newer.cancel.is_cancelled(),
            "now the second connection is oldest"
        );
        assert!(!newcomer.cancel.is_cancelled());
        assert!(!latest.cancel.is_cancelled());
    }

    #[test]
    fn ws_connection_counter_caps_each_user_independently() {
        let counter = WsConnectionCounter::new(1);
        let a = counter.acquire(Some("tenant-a"));
        let b = counter.acquire(Some("tenant-b"));

        assert!(
            !a.cancel.is_cancelled() && !b.cancel.is_cancelled(),
            "one connection each is within the cap"
        );

        let a_again = counter.acquire(Some("tenant-a"));
        assert!(a.cancel.is_cancelled(), "tenant-a is at its cap");
        assert!(
            !b.cancel.is_cancelled(),
            "one user at its cap must not affect another"
        );
        assert!(!a_again.cancel.is_cancelled());
        assert_eq!(counter.count("tenant-a"), 1);
        assert_eq!(counter.count("tenant-b"), 1);
    }

    #[test]
    fn ws_connection_counter_forgets_users_that_dropped_to_zero() {
        let counter = WsConnectionCounter::new(1);

        drop(counter.acquire(Some("tenant-a")));
        assert_eq!(counter.count("tenant-a"), 0);
        assert!(
            try_lock_users(&counter.users)
                .expect("uncontended")
                .is_empty(),
            "a churn of one-off users must not grow the map"
        );

        // Both guards are bound to names: one left as a temporary dies at the end of
        // its own statement and would free the slot it is meant to hold.
        let _b = counter.acquire(Some("tenant-b"));
        let _c = counter.acquire(Some("tenant-c"));
        assert_eq!(
            try_lock_users(&counter.users).expect("uncontended").len(),
            2
        );
    }

    #[test]
    fn acquire_gives_up_on_a_stuck_lock_instead_of_waiting_on_it() {
        let counter = WsConnectionCounter::new(1);
        let blocker = counter.users.lock();

        // From another thread: the lock is not reentrant, and the point is that a
        // caller returns on its own schedule rather than the lock holder's.
        let probe = Arc::clone(&counter);
        let guard = std::thread::spawn(move || probe.acquire(Some("tenant-a")))
            .join()
            .expect("acquire must return rather than wait for the lock");

        assert!(
            guard.user.is_none(),
            "the cap is a safety net, so an unavailable counter admits the              connection untracked rather than refusing or stalling it"
        );
        assert!(!guard.cancel.is_cancelled());

        drop(blocker);
        assert_eq!(counter.count("tenant-a"), 0);
    }

    #[test]
    fn ws_connection_counter_leaves_untracked_what_it_cannot_attribute() {
        let disabled = WsConnectionCounter::new(0);
        let mut held = Vec::new();
        for _ in 0..1000 {
            held.push(disabled.acquire(Some("tenant-a")));
        }
        assert!(
            held.iter().all(|guard| !guard.cancel.is_cancelled()),
            "limit 0 disables the cap"
        );
        assert_eq!(disabled.count("tenant-a"), 0);

        let counter = WsConnectionCounter::new(1);
        let first = counter.acquire(None);
        let second = counter.acquire(None);
        assert!(
            !first.cancel.is_cancelled() && !second.cancel.is_cancelled(),
            "unauthenticated connections are not attributable and stay untracked"
        );
    }

    #[tokio::test]
    async fn upload_writes_the_whole_body_before_handing_off_the_path() -> Result<(), CubeError> {
        let dir = tempfile::tempdir()?;
        let service = Arc::new(UploadStubService {
            dir: dir.path().to_path_buf(),
            seen: std::sync::Mutex::new(None),
        });

        // Many chunks well past the file's internal buffer: an unflushed tail
        // would show up here as a short read.
        let chunks: Vec<Vec<u8>> = (0..8u8).map(|i| vec![b'a' + i; 64 * 1024]).collect();
        let expected: Vec<u8> = chunks.iter().flatten().copied().collect();
        let body = futures::stream::iter(
            chunks
                .into_iter()
                .map(|c| Ok::<bytes::Bytes, warp::Error>(bytes::Bytes::from(c))),
        );

        HttpServer::handle_upload(
            service.clone(),
            SqlQueryContext::default(),
            UploadQuery {
                name: "upload.csv".to_string(),
            },
            body,
        )
        .await
        .map_err(|e| CubeError::internal(format!("{:?}", e)))?;

        let seen = service
            .seen
            .lock()
            .unwrap()
            .clone()
            .expect("upload_temp_file was never reached");
        // Compared without pretty_assertions to keep a failure from dumping 512KB.
        assert_eq!(seen.len(), expected.len());
        assert!(seen == expected, "uploaded bytes differ from the body");
        Ok(())
    }

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
    async fn query_test() -> Result<(), CubeError> {
        let message = HttpMessage {
            message_id: 1234,
            command: HttpCommand::Query {
                query: "test query".to_string(),
                inline_tables: vec![],
                trace_obj: Some("test trace".to_string()),
                parameters: None,
                response_format: QueryResultFormat::Legacy,
            },
            connection_id: Some("foo".to_string()),
        };
        let bytes = message.bytes();
        let output_message = HttpMessage::read(root_as_http_message(&bytes)?).await?;
        assert_eq!(message, output_message);
        Ok(())
    }

    #[tokio::test]
    async fn inline_tables_query_test() -> Result<(), CubeError> {
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
        let mut builder = cubeshared::flatbuffers::FlatBufferBuilder::with_capacity(1024);
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
                parameters: None,
                response_format: QueryResultFormat::Legacy,
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
        let message = HttpMessage::read(root_as_http_message(&bytes)?).await?;
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
                    trace_obj: None,
                    parameters: None,
                    response_format: QueryResultFormat::Legacy,
                },
                connection_id: Some("foo".to_string()),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn arrow_response_format_round_trip() -> Result<(), CubeError> {
        use crate::queryplanner::query_executor::batches_to_dataframe;
        use crate::sql::timestamp_from_string;
        use crate::util::decimal::{Decimal, Decimal96};
        use crate::util::int96::Int96;
        use cubeshared::codegen::{root_as_http_message, HttpQueryResultData};
        use datafusion::arrow::ipc::reader::StreamReader;
        use datafusion::arrow::record_batch::RecordBatch;

        // 1. Build a DataFrame with every TableValue variant + nulls.
        let columns = vec![
            Column::new("c_string".to_string(), ColumnType::String, 0),
            Column::new("c_int".to_string(), ColumnType::Int, 1),
            Column::new("c_int96".to_string(), ColumnType::Int96, 2),
            Column::new(
                "c_decimal".to_string(),
                ColumnType::Decimal {
                    scale: 4,
                    precision: 18,
                },
                3,
            ),
            Column::new(
                "c_decimal96".to_string(),
                ColumnType::Decimal96 {
                    scale: 6,
                    precision: 38,
                },
                4,
            ),
            Column::new("c_float".to_string(), ColumnType::Float, 5),
            Column::new("c_bytes".to_string(), ColumnType::Bytes, 6),
            Column::new("c_timestamp".to_string(), ColumnType::Timestamp, 7),
            Column::new("c_bool".to_string(), ColumnType::Boolean, 8),
        ];
        let rows = vec![
            Row::new(vec![
                TableValue::String("hello".to_string()),
                TableValue::Int(42),
                TableValue::Int96(Int96::new(123_456_789_012_345_i128)),
                TableValue::Decimal(Decimal::new(12345)),
                TableValue::Decimal96(Decimal96::new(67890)),
                TableValue::Float(3.5_f64.into()),
                TableValue::Bytes(vec![0x01, 0x02, 0x03]),
                TableValue::Timestamp(timestamp_from_string("2024-01-15T10:30:45.123Z")?),
                TableValue::Boolean(true),
            ]),
            Row::new(vec![
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
                TableValue::Null,
            ]),
        ];
        let original_df = Arc::new(DataFrame::new(columns.clone(), rows));

        // 2. Drive process_command with response_format = Arrow.
        let svc = Arc::new(StubService(original_df.clone()));
        let resp = HttpServer::process_command(
            svc,
            SqlQueryContext::default(),
            HttpCommand::Query {
                query: "select 1".to_string(),
                inline_tables: vec![],
                trace_obj: None,
                parameters: None,
                response_format: QueryResultFormat::Arrow,
            },
        )
        .await?;
        let arrow_bytes = match resp {
            HttpCommand::QueryResultArrow { data } => data,
            other => panic!("expected QueryResultArrow, got: {:?}", other),
        };

        // 3. Round-trip through HttpMessage::bytes() and verify the wire shape.
        let wire = HttpMessage {
            message_id: 99,
            command: HttpCommand::QueryResultArrow {
                data: arrow_bytes.clone(),
            },
            connection_id: None,
        }
        .bytes();
        let parsed = root_as_http_message(&wire)?;
        assert_eq!(
            parsed.command_type(),
            cubeshared::codegen::HttpCommand::HttpQueryResult
        );
        let result = parsed.command_as_http_query_result().unwrap();
        assert_eq!(
            result.data_type(),
            HttpQueryResultData::HttpQueryResultArrow
        );
        let arrow = result.data_as_http_query_result_arrow().unwrap();
        let payload: Vec<u8> = arrow.data().iter().collect();
        assert_eq!(payload, arrow_bytes);

        // 4. Decode the Arrow IPC stream, round-trip back to a DataFrame
        let reader = StreamReader::try_new(std::io::Cursor::new(payload), None).unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();

        let decoded = batches_to_dataframe(batches)?;
        // we don't compare directly both dataframes, because there is a difference with decimal96
        assert_eq!(decoded.get_columns().len(), original_df.get_columns().len());
        assert_eq!(decoded.get_rows().len(), original_df.get_rows().len());

        insta::assert_snapshot!("arrow_response_format_round_trip", decoded.print());

        Ok(())
    }

    #[tokio::test]
    async fn arrow_response_format_zero_columns_completed() -> Result<(), CubeError> {
        use cubeshared::codegen::{root_as_http_message, HttpQueryResultData};

        // Write commands (CREATE TABLE/INSERT, queue/cache writes) produce a
        // result with zero columns. For Arrow-format requests there's no Arrow
        // stream to build, so the server answers with QueryResultCompleted.
        let empty_df = Arc::new(DataFrame::new(vec![], vec![]));

        let svc = Arc::new(StubService(empty_df));
        let resp = HttpServer::process_command(
            svc,
            SqlQueryContext::default(),
            HttpCommand::Query {
                query: "CREATE TABLE s.t (id int)".to_string(),
                inline_tables: vec![],
                trace_obj: None,
                parameters: None,
                response_format: QueryResultFormat::Arrow,
            },
        )
        .await?;
        assert!(
            matches!(resp, HttpCommand::QueryResultCompleted),
            "expected QueryResultCompleted, got: {:?}",
            resp
        );

        // Round-trip through HttpMessage::bytes() and verify the wire shape.
        let wire = HttpMessage {
            message_id: 7,
            command: HttpCommand::QueryResultCompleted,
            connection_id: None,
        }
        .bytes();
        let parsed = root_as_http_message(&wire)?;
        assert_eq!(
            parsed.command_type(),
            cubeshared::codegen::HttpCommand::HttpQueryResult
        );
        let result = parsed.command_as_http_query_result().unwrap();
        assert_eq!(
            result.data_type(),
            HttpQueryResultData::HttpQueryResultCompleted
        );
        assert!(result.data_as_http_query_result_completed().is_some());

        Ok(())
    }

    pub struct SqlServiceMock {
        message_counter: AtomicU64,
    }

    crate::di_service!(SqlServiceMock, [SqlService]);

    #[async_trait]
    impl SqlService for SqlServiceMock {
        async fn exec_query(&self, _query: &str) -> Result<QueryResult, CubeError> {
            todo!()
        }

        async fn exec_query_with_context(
            &self,
            _context: SqlQueryContext,
            query: &str,
        ) -> Result<QueryResult, CubeError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let counter = self.message_counter.fetch_add(1, Ordering::Relaxed);
            if query == "close_connection" {
                Err(CubeError::wrong_connection("wrong connection".to_string()))
            } else if query == "error" {
                Err(CubeError::internal("error".to_string()))
            } else {
                Ok(QueryResult::Frame(Arc::new(DataFrame::new(
                    vec![Column::new("foo".to_string(), ColumnType::String, 0)],
                    vec![Row::new(vec![TableValue::String(format!("{}", counter))])],
                ))))
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
    async fn ws_test() -> Result<(), CubeError> {
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
            config.max_ws_connections_per_user(),
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
                            parameters: None,
                            response_format: QueryResultFormat::Legacy,
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
            let message = HttpMessage::read(root_as_http_message(&msg.into_data()).unwrap())
                .await
                .unwrap();
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
        Ok(())
    }

    #[test]
    fn check_process_id_header_bounds() {
        assert!(check_process_id_header(&None).is_ok());
        assert!(check_process_id_header(&Some("".to_string())).is_ok());
        assert!(
            check_process_id_header(&Some("x".repeat(QUEUE_ITEM_PROCESS_ID_MAX_LEN))).is_ok(),
            "the limit is inclusive"
        );

        let res = check_process_id_header(&Some("x".repeat(QUEUE_ITEM_PROCESS_ID_MAX_LEN + 1)));
        match res {
            Err(CubeRejection::BadRequest(msg)) => assert!(
                msg.contains("x-process-id header exceeds maximum allowed length"),
                "unexpected message: {}",
                msg
            ),
            other => panic!("Expected CubeRejection::BadRequest, actual: {:?}", other),
        }
    }

    #[tokio::test]
    async fn ws_process_id_header_test() -> Result<(), CubeError> {
        init_test_logger().await;

        let mut auth = MockSqlAuthService::new();
        auth.expect_authenticate().return_const(Ok(None));

        let config = Config::test("ws_process_id_header_test").config_obj();

        let http_server = Arc::new(HttpServer::new(
            "127.0.0.1:53032".to_string(),
            Arc::new(auth),
            Arc::new(SqlServiceMock {
                message_counter: AtomicU64::new(0),
            }),
            Duration::from_millis(100),
            Duration::from_millis(10000),
            Duration::from_millis(1000),
            config.transport_max_message_size(),
            config.transport_max_frame_size(),
            config.max_ws_connections_per_user(),
        ));
        {
            let http_server = http_server.clone();
            cube_ext::spawn(async move { http_server.run_server().await });
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        fn connect_request(process_id: &str) -> Request {
            let mut request = "ws://127.0.0.1:53032/ws".into_client_request().unwrap();
            request
                .headers_mut()
                .insert("x-process-id", HeaderValue::from_str(process_id).unwrap());
            request
        }

        let err = connect_async(connect_request(
            &"x".repeat(QUEUE_ITEM_PROCESS_ID_MAX_LEN + 1),
        ))
        .await
        .err()
        .expect("expected the handshake to be rejected");
        match err {
            WsError::Http(response) => assert_eq!(response.status().as_u16(), 400),
            other => panic!("Expected an http error, actual: {:?}", other),
        }

        let (socket, _) =
            connect_async(connect_request(&"x".repeat(QUEUE_ITEM_PROCESS_ID_MAX_LEN)))
                .await
                .expect("a header at the limit should be accepted");
        drop(socket);

        http_server.stop_processing().await;
        Ok(())
    }

    /// An evicted connection is told why it is going away, so a client can tell
    /// being recycled from a network blip, and the cap actually reaches the
    /// connection rather than only the counter.
    #[tokio::test]
    async fn ws_connection_evicted_test() -> Result<(), CubeError> {
        init_test_logger().await;

        let mut auth = MockSqlAuthService::new();
        auth.expect_authenticate().return_const(Ok(None));

        let http_server = Arc::new(HttpServer::new(
            "127.0.0.1:53036".to_string(),
            Arc::new(auth),
            Arc::new(SqlServiceMock {
                message_counter: AtomicU64::new(0),
            }),
            Duration::from_millis(100),
            Duration::from_millis(10000),
            Duration::from_millis(1000),
            64 * 1024,
            64 * 1024,
            // One connection per user, so the second one has to evict the first.
            1,
        ));
        {
            let http_server = http_server.clone();
            cube_ext::spawn(async move { http_server.run_server().await });
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        fn connect_request(user: &str) -> Request {
            let mut request = "ws://127.0.0.1:53036/ws".into_client_request().unwrap();
            request.headers_mut().insert(
                "authorization",
                HeaderValue::from_str(&Credentials::new(user, "").as_http_header()).unwrap(),
            );
            request
        }

        let (mut first, _) = connect_async(connect_request("tenant-a"))
            .await
            .expect("the first connection is within the cap");
        let (second, _) = connect_async(connect_request("tenant-a"))
            .await
            .expect("the second connection is admitted by evicting the first");

        // Bounded, so a regression in the eviction path fails this test instead of
        // hanging it: nothing else would ever wake this read.
        let msg = tokio::time::timeout(Duration::from_secs(10), first.next())
            .await
            .expect("the evicted connection must be closed promptly")
            .expect("the evicted connection is closed, not left open")
            .unwrap();
        match msg {
            Message::Close(Some(frame)) => {
                assert_eq!(u16::from(frame.code), CONNECTION_EVICTED_CLOSE_CODE);
                assert!(
                    frame.reason.contains("evicted"),
                    "unexpected close reason: {}",
                    frame.reason
                );
            }
            msg => panic!("Close frame expected, got: {:?}", msg),
        }

        // Another user is unaffected by tenant-a having been at its cap.
        let (other, _) = connect_async(connect_request("tenant-b"))
            .await
            .expect("a different user has its own slot");

        drop(other);
        drop(second);

        http_server.stop_processing().await;
        Ok(())
    }

    /// An incoming message past the transport backstop is answered with the
    /// WebSocket "message too big" close code instead of the connection being
    /// dropped without a word, which the client can only read as a bare
    /// disconnect and retry.
    #[tokio::test]
    async fn ws_message_too_large_test() -> Result<(), CubeError> {
        init_test_logger().await;

        let max_message_size = 4 * 1024;
        let mut auth = MockSqlAuthService::new();
        auth.expect_authenticate().return_const(Ok(None));

        let http_server = Arc::new(HttpServer::new(
            "127.0.0.1:53033".to_string(),
            Arc::new(auth),
            Arc::new(SqlServiceMock {
                message_counter: AtomicU64::new(0),
            }),
            Duration::from_millis(100),
            Duration::from_millis(10000),
            Duration::from_millis(1000),
            max_message_size,
            max_message_size,
            0, // no websocket connection cap
        ));
        {
            let http_server = http_server.clone();
            cube_ext::spawn(async move { http_server.run_server().await });
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (mut socket, _) = connect_async(Url::parse("ws://127.0.0.1:53033/ws").unwrap())
            .await
            .unwrap();

        // Clear of the headroom the graceful path is given, so that the server
        // refuses the frame before it can read the message id back out of it.
        socket
            .send(Message::binary(
                HttpMessage {
                    message_id: 1,
                    command: HttpCommand::Query {
                        query: "s".repeat(max_message_size * TRANSPORT_SIZE_HEADROOM * 2),
                        inline_tables: vec![],
                        trace_obj: None,
                        parameters: None,
                        response_format: QueryResultFormat::Legacy,
                    },
                    connection_id: Some("foo".to_string()),
                }
                .bytes(),
            ))
            .await
            .unwrap();

        let msg = socket.next().await.unwrap().unwrap();
        match msg {
            Message::Close(Some(frame)) => {
                assert_eq!(u16::from(frame.code), MESSAGE_TOO_BIG_CLOSE_CODE);
                // The configured limit, not the backstop the transport is
                // given: that is the number an operator set and can change.
                assert!(
                    frame.reason.contains(&format!(
                        "exceeds the maximum message size of {} bytes",
                        max_message_size
                    )),
                    "unexpected close reason: {}",
                    frame.reason
                );
            }
            msg => panic!("Close frame expected, got: {:?}", msg),
        }

        http_server.stop_processing().await;
        Ok(())
    }

    /// The close reason names whichever limit refused the message. The same
    /// capacity error covers both, so a frame limit configured below the
    /// message limit would otherwise be reported as a message limit that never
    /// refused anything.
    #[tokio::test]
    async fn ws_message_too_large_names_the_frame_limit_test() -> Result<(), CubeError> {
        init_test_logger().await;

        let max_frame_size = 4 * 1024;
        let max_message_size = max_frame_size * 4;
        let mut auth = MockSqlAuthService::new();
        auth.expect_authenticate().return_const(Ok(None));

        let http_server = Arc::new(HttpServer::new(
            "127.0.0.1:53035".to_string(),
            Arc::new(auth),
            Arc::new(SqlServiceMock {
                message_counter: AtomicU64::new(0),
            }),
            Duration::from_millis(100),
            Duration::from_millis(10000),
            Duration::from_millis(1000),
            max_message_size,
            max_frame_size,
            0, // no websocket connection cap
        ));
        {
            let http_server = http_server.clone();
            cube_ext::spawn(async move { http_server.run_server().await });
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (mut socket, _) = connect_async(Url::parse("ws://127.0.0.1:53035/ws").unwrap())
            .await
            .unwrap();

        // Past the frame backstop but inside the message one, so the frame
        // limit is what refuses it.
        socket
            .send(Message::binary(
                HttpMessage {
                    message_id: 1,
                    command: HttpCommand::Query {
                        query: "s".repeat(max_frame_size * TRANSPORT_SIZE_HEADROOM * 2),
                        inline_tables: vec![],
                        trace_obj: None,
                        parameters: None,
                        response_format: QueryResultFormat::Legacy,
                    },
                    connection_id: Some("foo".to_string()),
                }
                .bytes(),
            ))
            .await
            .unwrap();

        let msg = socket.next().await.unwrap().unwrap();
        match msg {
            Message::Close(Some(frame)) => {
                assert_eq!(u16::from(frame.code), MESSAGE_TOO_BIG_CLOSE_CODE);
                assert!(
                    frame.reason.contains(&format!(
                        "exceeds the maximum frame size of {} bytes",
                        max_frame_size
                    )),
                    "unexpected close reason: {}",
                    frame.reason
                );
            }
            msg => panic!("Close frame expected, got: {:?}", msg),
        }

        http_server.stop_processing().await;
        Ok(())
    }

    /// An over-limit request that still fits within the transport headroom is
    /// answered with an error naming the message it belongs to, and the
    /// connection carries on serving the queries multiplexed over it.
    #[tokio::test]
    async fn ws_message_too_large_reports_the_message_test() -> Result<(), CubeError> {
        init_test_logger().await;

        let max_message_size = 4 * 1024;
        let mut auth = MockSqlAuthService::new();
        auth.expect_authenticate().return_const(Ok(None));

        let http_server = Arc::new(HttpServer::new(
            "127.0.0.1:53034".to_string(),
            Arc::new(auth),
            Arc::new(SqlServiceMock {
                message_counter: AtomicU64::new(0),
            }),
            Duration::from_millis(100),
            Duration::from_millis(10000),
            Duration::from_millis(1000),
            max_message_size,
            max_message_size,
            0, // no websocket connection cap
        ));
        {
            let http_server = http_server.clone();
            cube_ext::spawn(async move { http_server.run_server().await });
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (mut socket, _) = connect_async(Url::parse("ws://127.0.0.1:53034/ws").unwrap())
            .await
            .unwrap();

        // Over the limit, but inside the headroom the transport is given, so it
        // arrives whole and can be attributed to message 7.
        socket
            .send(Message::binary(
                HttpMessage {
                    message_id: 7,
                    command: HttpCommand::Query {
                        query: "s".repeat(max_message_size + max_message_size / 2),
                        inline_tables: vec![],
                        trace_obj: None,
                        parameters: None,
                        response_format: QueryResultFormat::Legacy,
                    },
                    connection_id: Some("foo".to_string()),
                }
                .bytes(),
            ))
            .await
            .unwrap();

        // Read off the flatbuffer directly: `HttpMessage::read` only decodes the
        // commands a client sends, and an error is not one of them.
        let msg = socket.next().await.unwrap().unwrap();
        let data = msg.into_data();
        let message = root_as_http_message(&data).unwrap();
        assert_eq!(message.message_id(), 7);
        let error = message
            .command_as_http_error()
            .expect("an error was expected")
            .error()
            .unwrap_or_default();
        assert!(
            error.contains(&format!(
                "exceeds the maximum message size of {} bytes",
                max_message_size
            )),
            "unexpected error: {}",
            error
        );

        // The point of answering instead of closing: the connection is still
        // usable for everything that wasn't oversized.
        socket
            .send(Message::binary(
                HttpMessage {
                    message_id: 8,
                    command: HttpCommand::Query {
                        query: "foo".to_string(),
                        inline_tables: vec![],
                        trace_obj: None,
                        parameters: None,
                        response_format: QueryResultFormat::Legacy,
                    },
                    connection_id: Some("foo".to_string()),
                }
                .bytes(),
            ))
            .await
            .unwrap();

        let msg = socket.next().await.unwrap().unwrap();
        let message = HttpMessage::read(root_as_http_message(&msg.into_data()).unwrap())
            .await
            .unwrap();
        assert_eq!(message.message_id, 8);
        match message.command {
            HttpCommand::ResultSet { data_frame } => assert_eq!(
                data_frame.get_rows()[0].values()[0],
                TableValue::String("0".to_string())
            ),
            command => panic!("Result set expected, got: {:?}", command),
        }

        http_server.stop_processing().await;
        Ok(())
    }
}
