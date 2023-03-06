pub mod tracing;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use core::mem;
use core::time::Duration;
use datafusion::cube_ext;
use deflate::deflate_bytes_zlib;
use futures::{Sink, StreamExt};
use futures_timer::Delay;
use log::{Level, Log, Metadata, Record};
use nanoid::nanoid;
use reqwest::header::HeaderMap;
use serde_json::{Map, Number, Value};
use std::collections::{HashMap, HashSet};
use std::env;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

lazy_static! {
    pub static ref SENDER: Arc<EventSender> = Arc::new(EventSender::new(Arc::new(
        HttpTelemetryTransport::try_new("https://track.cube.dev/track".to_string(), None).unwrap()
    )));
}

lazy_static! {
    pub static ref AGENT_SENDER: tokio::sync::RwLock<Option<Arc<EventSender>>> =
        tokio::sync::RwLock::new(None);
}

pub struct EventSender {
    events: Mutex<Vec<Map<String, Value>>>,
    notify: Arc<Notify>,
    stopped: RwLock<bool>,
    transport: Arc<dyn TelemetryTransport>,
}

#[async_trait]
pub trait TelemetryTransport: Sync + Send {
    async fn send_events(&self, to_send: Vec<Map<String, Value>>) -> Result<(), CubeError>;
}

#[derive(Debug)]
pub struct HttpTelemetryTransport {
    endpoint_url: String,
    client: reqwest::Client,
    headers: Option<HeaderMap>,
}

impl HttpTelemetryTransport {
    pub fn try_new(endpoint_url: String, headers: Option<HeaderMap>) -> Result<Self, CubeError> {
        let client = reqwest::ClientBuilder::new()
            .use_rustls_tls()
            .user_agent("cubestore")
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .build()?;
        Ok(Self {
            endpoint_url,
            client,
            headers,
        })
    }
}

#[async_trait]
impl TelemetryTransport for HttpTelemetryTransport {
    async fn send_events(&self, mut to_send: Vec<Map<String, Value>>) -> Result<(), CubeError> {
        let max_retries = 10usize;
        for retry in 0..max_retries {
            let sent_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            for event in to_send.iter_mut() {
                event.insert("sentAt".to_string(), Value::String(sent_at.to_string()));
            }

            log::trace!("sending via http to {} :{:?}", self.endpoint_url, to_send);

            let header_map = self
                .headers
                .as_ref()
                .map_or_else(|| HeaderMap::new(), |m| m.to_owned());
            let res = self
                .client
                .post(&self.endpoint_url)
                .headers(header_map)
                .json(&to_send)
                .send()
                .await?;

            if res.status() != 200 {
                if retry < max_retries - 1 {
                    continue;
                } else {
                    return Err(CubeError::internal(format!(
                        "Send events error: {}",
                        res.text().await?
                    )));
                }
            } else {
                return Ok(());
            }
        }
        Err(CubeError::internal(format!(
            "Send events error: shouldn't get there"
        )))
    }
}

pub struct WsTelemetryTransport {
    endpoint_url: String,
    socket: RwLock<Option<Pin<Box<WebSocketStream<MaybeTlsStream<TcpStream>>>>>>,
    waiting_callbacks: RwLock<HashSet<String>>,
}

impl WsTelemetryTransport {
    async fn connect(&self) -> Result<(), CubeError> {
        if self.socket.read().await.is_none() {
            let mut socket = self.socket.write().await;
            if socket.is_none() {
                let (new, _) = tokio_tungstenite::connect_async(&self.endpoint_url).await?;
                *socket = Some(Box::pin(new))
            }
        }
        Ok(())
    }

    async fn close_socket(&self) {
        let mut socket = self.socket.write().await;
        if let Some(socket) = socket.as_mut() {
            if let Err(e) = socket.close(None).await {
                log::error!("Error during agent web socket close: {}", e);
            }
        }
        *socket = None;
    }

    async fn remove_callback(&self, callback_id: &str) {
        let mut callbacks = self.waiting_callbacks.write().await;
        callbacks.remove(callback_id);
    }

    async fn insert_callback(&self, callback_id: String) {
        let mut callbacks = self.waiting_callbacks.write().await;
        callbacks.insert(callback_id);
    }

    async fn has_callback(&self, callback_id: &str) -> bool {
        let callbacks = self.waiting_callbacks.read().await;
        callbacks.contains(callback_id)
    }
}

#[async_trait]
impl TelemetryTransport for WsTelemetryTransport {
    async fn send_events(&self, mut to_send: Vec<Map<String, Value>>) -> Result<(), CubeError> {
        for try_num in 1..5 {
            if let Err(e) = self.connect().await {
                log::error!("Error during agent web socket connect: {}", e);
                break;
            }
            let sent_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            for event in to_send.iter_mut() {
                event.insert("sentAt".to_string(), Value::String(sent_at.to_string()));
            }
            let mut data_obj = Map::new();
            data_obj.insert(
                "data".to_string(),
                Value::Array(to_send.iter().map(|v| Value::Object(v.clone())).collect()),
            );
            let mut message = Map::new();
            message.insert("method".to_string(), Value::String("agent".to_string()));
            message.insert("params".to_string(), Value::Object(data_obj));
            let callback_id = nanoid!(16);
            message.insert(
                "callbackId".to_string(),
                Value::String(callback_id.to_string()),
            );
            log::trace!("sending via ws to {} :{:?}", self.endpoint_url, message);
            let json = deflate_bytes_zlib(serde_json::to_vec(&message)?.as_slice());

            self.insert_callback(callback_id.to_string()).await;

            if let Some(socket) = self.socket.write().await.as_mut() {
                if let Err(e) = socket.as_mut().start_send(Message::Binary(json)) {
                    log::error!("Error during agent send: {}", e);
                    self.remove_callback(&callback_id).await;
                    self.close_socket().await;
                    continue;
                }
            }

            let mut deadline = Delay::new(Duration::from_secs(15 * try_num));
            loop {
                if let Some(socket) = self.socket.write().await.as_mut() {
                    let mut socket = socket.as_mut();
                    let response = tokio::select! {
                        response = socket.next() => {
                            response
                        }
                        _ = &mut deadline => {
                            break;
                        }
                    };
                    if let Some(res) = response {
                        match res {
                            Ok(msg) => {
                                if let Message::Text(message_text) = msg {
                                    match serde_json::from_str::<Value>(&message_text) {
                                        Ok(value) => {
                                            if let Value::Object(response_obj) = value {
                                                if let Some(Value::Object(params_obj)) =
                                                    response_obj.get("params")
                                                {
                                                    if let Some(Value::String(
                                                        received_callback_id,
                                                    )) = params_obj.get("callbackId")
                                                    {
                                                        self.remove_callback(received_callback_id)
                                                            .await;
                                                        if received_callback_id == &callback_id {
                                                            break;
                                                        }
                                                    }
                                                } else {
                                                    log::trace!(
                                                        "Unrecognized response from agent server: {}",
                                                        message_text
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            log::error!("Error during agent web socket json parse of '{}': {}", message_text, e);
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("Error during agent web socket read: {}", e);
                                break;
                            }
                        }
                    }
                }
            }

            let has_callback = self.has_callback(&callback_id).await;
            self.remove_callback(&callback_id).await;

            // Parallel read has received message and removed it from callbacks
            if !has_callback {
                log::trace!("Sent {} agent events", to_send.len());
                return Ok(());
            }

            // Timeout or error -- closing socket
            self.close_socket().await;
        }
        Err(CubeError::internal(format!(
            "Can't send agent events: {:?}",
            to_send
        )))
    }
}

impl EventSender {
    pub fn new(transport: Arc<dyn TelemetryTransport>) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            notify: Arc::new(Notify::new()),
            stopped: RwLock::new(false),
            transport,
        }
    }

    pub async fn track_event(&self, event: String, properties: HashMap<String, String>) {
        let mut obj = Map::new();
        for (k, v) in properties.iter() {
            obj.insert(k.to_string(), Value::String(v.to_string()));
        }
        self.track_event_object(event, obj, false).await
    }

    pub async fn track_event_object(
        &self,
        event: String,
        obj: Map<String, Value>,
        is_agent_event: bool,
    ) {
        let mut properties = obj.clone();
        if is_agent_event {
            properties.insert("msg".to_string(), Value::String(event));
        } else {
            properties.insert("event".to_string(), Value::String(event));
        }
        properties.insert("id".to_string(), Value::String(nanoid!(16)));
        if is_agent_event {
            properties.insert(
                "timestamp".to_string(),
                Value::String(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)),
            );
        } else {
            properties.insert(
                "anonymousId".to_string(),
                Value::String("cubestore".to_string()),
            );
            properties.insert(
                "clientTimestamp".to_string(),
                Value::String(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)),
            );
        }
        self.events.lock().await.push(properties);
        self.notify.notify_waiters();
    }

    pub async fn send_loop(&self) {
        loop {
            tokio::select! {
                _ = self.notify.notified() => {
                    ()
                }
                _ = Delay::new(Duration::from_secs(5)) => {
                    ()
                }
            };
            if *self.stopped.read().await {
                return;
            }
            let mut to_send = vec![];
            {
                let mut events = self.events.lock().await;
                mem::swap(&mut to_send, &mut events);
            }
            if !to_send.is_empty() {
                if let Err(_) = self.transport.send_events(to_send).await {
                    // println!("Send Error: {}", e);
                }
            }
        }
    }

    pub async fn stop_loop(&self) {
        let mut stopped = self.stopped.write().await;
        *stopped = true;
        self.notify.notify_waiters();
    }
}

pub async fn track_event(event: String, properties: HashMap<String, String>) {
    SENDER.track_event(event, properties).await;
}

pub fn track_event_spawn(event: String, properties: HashMap<String, String>) {
    cube_ext::spawn(async move { SENDER.track_event(event, properties).await });
}

pub fn agent_event_spawn(event: String, properties: Map<String, Value>) {
    cube_ext::spawn(async move {
        if let Some(sender) = AGENT_SENDER.read().await.as_ref() {
            sender.track_event_object(event, properties, true).await;
        }
    });
}

pub fn incoming_traffic_agent_event(trace_obj: &str, bytes: u64) -> Result<(), CubeError> {
    let obj: Value = serde_json::from_str(trace_obj)?;
    if let Value::Object(mut obj) = obj {
        obj.insert(
            "service".to_string(),
            Value::String("cubestore".to_string()),
        );
        obj.insert(
            "bytes".to_string(),
            Value::Number(Number::from_f64(bytes as f64).unwrap()),
        );
        agent_event_spawn("Incoming network usage".to_string(), obj);
        Ok(())
    } else {
        Err(CubeError::user(format!(
            "Trace object expected to be a JSON object but found: {}",
            trace_obj
        )))
    }
}

pub async fn start_track_event_loop() {
    let sender = SENDER.clone();
    sender.send_loop().await;
}

pub async fn stop_track_event_loop() {
    SENDER.stop_loop().await;
}

pub async fn init_agent_sender() {
    let agent_url = env::var("CUBESTORE_AGENT_ENDPOINT_URL").ok();
    log::trace!("agent endpoint url: {:?}", agent_url);
    let mut agent_sender = AGENT_SENDER.write().await;
    *agent_sender = if let Some(endpoint_url) = agent_url {
        if let Ok(agent_url_object) = reqwest::Url::parse(endpoint_url.as_str()) {
            match agent_url_object.scheme() {
                "https" | "http" => {
                    log::trace!("using http transport for agent enpoint");
                    Some(Arc::new(EventSender::new(Arc::new(
                        HttpTelemetryTransport::try_new(endpoint_url, None).unwrap(),
                    ))))
                }
                "wss" => {
                    log::trace!("using wss transport for agent enpoint");
                    Some(Arc::new(EventSender::new(Arc::new(WsTelemetryTransport {
                        endpoint_url,
                        socket: RwLock::new(None),
                        waiting_callbacks: RwLock::new(HashSet::new()),
                    }))))
                }
                _ => {
                    log::error!(
                        "Telemetry endpoint {} with unsupported protocol",
                        endpoint_url
                    );
                    None
                }
            }
        } else {
            log::error!("Can't parse telemetry endpoint {}", endpoint_url);
            None
        }
    } else {
        None
    };
}

pub async fn start_agent_event_loop() {
    if let Some(sender) = AGENT_SENDER.read().await.as_ref() {
        sender.clone().send_loop().await;
    }
}
pub async fn stop_agent_event_loop() {
    if let Some(sender) = AGENT_SENDER.read().await.as_ref() {
        sender.clone().stop_loop().await;
    }
}

pub struct ReportingLogger {
    logger: Box<dyn Log>,
}

impl ReportingLogger {
    pub fn new(logger: Box<dyn Log>) -> Self {
        Self { logger }
    }
}

impl Log for ReportingLogger {
    fn enabled<'a>(&self, metadata: &Metadata<'a>) -> bool {
        self.logger.enabled(metadata)
    }

    fn log<'a>(&self, record: &Record<'a>) {
        if let Level::Error = record.metadata().level() {
            track_event_spawn(
                "Cube Store Error".to_string(),
                vec![("error".to_string(), format!("{}", record.args()))]
                    .into_iter()
                    .collect(),
            )
        }
        self.logger.log(record)
    }

    fn flush(&self) {
        self.logger.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION};

    #[test]
    fn test_http_telemetry_transport_try_new() {
        let transport_without_headers =
            HttpTelemetryTransport::try_new("http://transport_without_headers".to_string(), None)
                .unwrap();

        assert_eq!(
            transport_without_headers.endpoint_url,
            "http://transport_without_headers"
        );
        assert_eq!(transport_without_headers.headers, None);

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str("token").unwrap());
        headers.insert(
            HeaderName::from_static("content-length"),
            HeaderValue::from_str("10000").unwrap(),
        );

        let transport_with_headers = HttpTelemetryTransport::try_new(
            "http://transport_with_header".to_string(),
            Some(headers.clone()),
        )
        .unwrap();

        assert_eq!(
            transport_with_headers.endpoint_url,
            "http://transport_with_header"
        );

        assert_eq!(
            format!("{:?}", transport_with_headers.headers.unwrap()),
            Value::String(
                "{\"authorization\": \"token\", \"content-length\": \"10000\"}".to_string()
            )
        );
    }
}
