use std::collections::HashMap;
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{HeaderValue, Request};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{connect_async_with_config, MaybeTlsStream, WebSocketStream};

use crate::client::ClientConfig;
use crate::codec::{decode_frame, encode_query, DecodedResponse};
use crate::error::TransportError;
use crate::result::QueryResult;

pub(crate) type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) enum ActorRequest {
    Query {
        sql: String,
        reply: oneshot::Sender<Result<QueryResult, TransportError>>,
    },
    Close,
}

struct PendingQuery {
    reply: oneshot::Sender<Result<QueryResult, TransportError>>,
    buffer: Bytes,
}

pub(crate) struct Actor {
    cfg: ClientConfig,
    connection_id: String,
    process_id: String,
    next_msg_id: u32,
    pending: HashMap<u32, PendingQuery>,
    pending_resend: Vec<Bytes>,
    inbox: mpsc::UnboundedReceiver<ActorRequest>,
    ws: Option<WsStream>,
    last_pong: Instant,
}

impl Actor {
    pub(crate) fn new(
        cfg: ClientConfig,
        connection_id: String,
        process_id: String,
        inbox: mpsc::UnboundedReceiver<ActorRequest>,
        ws: WsStream,
    ) -> Self {
        Self {
            cfg,
            connection_id,
            process_id,
            next_msg_id: 1,
            pending: HashMap::new(),
            pending_resend: Vec::new(),
            inbox,
            ws: Some(ws),
            last_pong: Instant::now(),
        }
    }

    pub(crate) async fn run(mut self) {
        'outer: loop {
            let Some(ws) = self.ws.take() else {
                break;
            };

            let (mut sink, mut stream) = ws.split();
            self.last_pong = Instant::now();

            // After a reconnect: flush any unanswered buffers with their original message ids.
            // Matches WebSocketConnection.ts:128-143.
            let resend: Vec<Bytes> = std::mem::take(&mut self.pending_resend);
            for buf in resend {
                if let Err(e) = sink.send(Message::Binary(buf)).await {
                    log::warn!("resend after reconnect failed: {e}");
                    self.requeue_pending_for_resend();
                    if !self.attempt_reconnect().await {
                        break 'outer;
                    }
                    continue 'outer;
                }
            }

            let mut ping_interval = tokio::time::interval(self.cfg.ping_interval);
            ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // Burn the immediate first tick so we don't ping the instant we connect.
            ping_interval.tick().await;

            let disconnected = loop {
                tokio::select! {
                    biased;

                    maybe_req = self.inbox.recv() => {
                        match maybe_req {
                            None | Some(ActorRequest::Close) => {
                                let _ = sink.send(Message::Close(None)).await;
                                self.fail_all_pending(TransportError::Closed);
                                return;
                            }
                            Some(ActorRequest::Query { sql, reply }) => {
                                let msg_id = self.next_msg_id;
                                self.next_msg_id = self.next_msg_id.wrapping_add(1).max(1);
                                let buf = encode_query(msg_id, &self.connection_id, &sql);
                                self.pending.insert(msg_id, PendingQuery { reply, buffer: buf.clone() });
                                if let Err(e) = sink.send(Message::Binary(buf)).await {
                                    log::warn!("send failed, will reconnect: {e}");
                                    break true;
                                }
                            }
                        }
                    }

                    maybe_msg = stream.next() => {
                        match maybe_msg {
                            None => {
                                log::warn!("websocket stream ended");
                                break true;
                            }
                            Some(Err(e)) => {
                                log::warn!("websocket error: {e}");
                                break true;
                            }
                            Some(Ok(Message::Binary(bytes))) => {
                                match decode_frame(&bytes) {
                                    Ok(frame) => {
                                        if let Some(pending) = self.pending.remove(&frame.message_id) {
                                            let result = match frame.response {
                                                DecodedResponse::Ok(qr) => Ok(qr),
                                                DecodedResponse::Error(msg) => Err(TransportError::Query(msg)),
                                            };
                                            let _ = pending.reply.send(result);
                                        } else {
                                            log::warn!("unsolicited message id {}", frame.message_id);
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!("frame decode error: {e}");
                                    }
                                }
                            }
                            Some(Ok(Message::Pong(_))) => {
                                self.last_pong = Instant::now();
                            }
                            Some(Ok(Message::Ping(payload))) => {
                                if let Err(e) = sink.send(Message::Pong(payload)).await {
                                    log::warn!("pong send failed: {e}");
                                    break true;
                                }
                            }
                            Some(Ok(Message::Close(_))) => {
                                log::info!("websocket closed by server");
                                break true;
                            }
                            Some(Ok(_)) => {
                                // Frame / text — ignore.
                            }
                        }
                    }

                    _ = ping_interval.tick() => {
                        if self.last_pong.elapsed() > self.cfg.no_heartbeat_timeout {
                            log::warn!("heartbeat timeout — reconnecting");
                            break true;
                        }
                        if let Err(e) = sink.send(Message::Ping(Bytes::new())).await {
                            log::warn!("ping send failed: {e}");
                            break true;
                        }
                    }
                }
            };

            drop(sink);
            drop(stream);

            if !disconnected {
                break;
            }

            self.requeue_pending_for_resend();
            if !self.attempt_reconnect().await {
                break;
            }
        }

        self.fail_all_pending(TransportError::Disconnected);
    }

    fn requeue_pending_for_resend(&mut self) {
        self.pending_resend.clear();
        self.pending_resend
            .extend(self.pending.values().map(|p| p.buffer.clone()));
    }

    fn fail_all_pending(&mut self, err: TransportError) {
        let pending = std::mem::take(&mut self.pending);
        for (_, p) in pending {
            // err is not Clone — synthesize a fresh equivalent for each.
            let e = match &err {
                TransportError::Disconnected => TransportError::Disconnected,
                TransportError::Closed => TransportError::Closed,
                other => TransportError::Protocol(other.to_string()),
            };
            let _ = p.reply.send(Err(e));
        }
    }

    async fn attempt_reconnect(&mut self) -> bool {
        for attempt in 0..self.cfg.max_connect_retries {
            let wait = Duration::from_millis((attempt as u64 + 1) * 1000);
            tokio::time::sleep(wait).await;
            match connect_ws(&self.cfg, &self.process_id).await {
                Ok((ws, _version)) => {
                    self.ws = Some(ws);
                    return true;
                }
                Err(e) => {
                    log::warn!(
                        "reconnect attempt {}/{} failed: {}",
                        attempt + 1,
                        self.cfg.max_connect_retries,
                        e
                    );
                }
            }
        }
        false
    }
}

/// Build an HTTP upgrade request, perform a WebSocket handshake to cubestore, and
/// return both the stream and the server's `X-CubeStore-Version` header.
pub(crate) async fn connect_ws(
    cfg: &ClientConfig,
    process_id: &str,
) -> Result<(WsStream, Option<String>), TransportError> {
    let ws_url = build_ws_url(&cfg.url)?;

    let mut builder = Request::builder().method("GET").uri(ws_url.as_str());

    // Required WebSocket headers (tokio-tungstenite injects most but be explicit for clarity).
    builder = builder
        .header(
            "Host",
            host_header(&cfg.url).ok_or_else(|| {
                TransportError::InvalidUrl(format!("missing host in url: {}", cfg.url))
            })?,
        )
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        );

    // RFC 7617 allows either side of `user:pass` to be empty (token-style auth
    // is commonly sent as `:<token>`), so emit the header whenever any credential
    // is configured rather than silently dropping it when one side is missing.
    if cfg.username.is_some() || cfg.password.is_some() {
        let user = cfg.username.as_deref().unwrap_or("");
        let pass = cfg.password.as_deref().unwrap_or("");

        let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
        let value = HeaderValue::from_str(&format!("Basic {token}"))
            .map_err(|e| TransportError::Auth(e.to_string()))?;
        builder = builder.header("Authorization", value);
    }

    let truncated: String = process_id.chars().take(64).collect();
    let value = HeaderValue::from_str(&truncated)
        .map_err(|e| TransportError::Auth(format!("x-process-id: {e}")))?;
    builder = builder.header("x-process-id", value);

    let request = builder
        .body(())
        .map_err(|e| TransportError::InvalidUrl(e.to_string()))?;

    // Match cubestore's transport caps (default 64MiB message / 32MiB frame; the
    // server can be configured up to 256MiB). The tungstenite default of 16MiB
    // per frame is too tight for large query results.
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(256 << 20))
        .max_frame_size(Some(256 << 20));
    let connect_future = connect_async_with_config(request, Some(ws_config), false);
    let (ws, response) = tokio::time::timeout(cfg.connect_timeout, connect_future)
        .await
        .map_err(|_| {
            TransportError::Connect(tokio_tungstenite::tungstenite::Error::Io(
                std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"),
            ))
        })??;

    let version = response
        .headers()
        .get("X-CubeStore-Version")
        .or_else(|| response.headers().get("x-cubestore-version"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    Ok((ws, version))
}

fn build_ws_url(base: &url::Url) -> Result<url::Url, TransportError> {
    let mut u = base.clone();
    let path = u.path();

    // If the user already pointed at /ws (or another explicit path), keep it.
    // Otherwise append /ws.
    if path == "/" || path.is_empty() {
        u.set_path("/ws");
    }

    let scheme = u.scheme();
    if scheme != "ws" && scheme != "wss" {
        return Err(TransportError::InvalidUrl(format!(
            "expected ws:// or wss://, got {scheme}://"
        )));
    }

    Ok(u)
}

fn host_header(url: &url::Url) -> Option<String> {
    let host = url.host_str()?;
    match url.port() {
        Some(p) => Some(format!("{host}:{p}")),
        None => Some(host.to_string()),
    }
}
