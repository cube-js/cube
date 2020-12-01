use crate::CubeError;
use chrono::{SecondsFormat, Utc};
use core::mem;
use log::{Level, LevelFilter, Log, Metadata, Record};
use nanoid::nanoid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};

lazy_static! {
    pub static ref SENDER: Arc<EventSender> = Arc::new(EventSender::new());
}

pub struct EventSender {
    events: Mutex<Vec<HashMap<String, String>>>,
    notify: Arc<Notify>,
    stopped: RwLock<bool>,
}

impl EventSender {
    pub fn new() -> Self {
        EventSender {
            events: Mutex::new(Vec::new()),
            notify: Arc::new(Notify::new()),
            stopped: RwLock::new(false),
        }
    }

    pub async fn track_event(&self, event: String, mut properties: HashMap<String, String>) {
        properties.insert("event".to_string(), event);
        properties.insert("id".to_string(), nanoid!(16));
        properties.insert("anonymousId".to_string(), "cubestore".to_string());
        properties.insert(
            "clientTimestamp".to_string(),
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        );
        self.events.lock().await.push(properties);
        self.notify.notify();
    }

    pub async fn send_loop(&self) {
        loop {
            self.notify.notified().await;
            if *self.stopped.read().await {
                return;
            }
            let mut to_send = vec![];
            {
                let mut events = self.events.lock().await;
                mem::swap(&mut to_send, &mut events);
            }
            if let Err(_) = EventSender::send_events(to_send).await {
                // println!("Send Error: {}", e);
            }
        }
    }

    pub async fn stop_loop(&self) {
        let mut stopped = self.stopped.write().await;
        *stopped = true;
        self.notify.notify();
    }

    async fn send_events(mut to_send: Vec<HashMap<String, String>>) -> Result<(), CubeError> {
        let max_retries = 10usize;
        for retry in 0..max_retries {
            let client = reqwest::ClientBuilder::new()
                .use_rustls_tls()
                .user_agent("cubestore")
                .build()
                .unwrap();

            let sent_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            for event in to_send.iter_mut() {
                event.insert("sentAt".to_string(), sent_at.to_string());
            }

            let res = client
                .post("https://track.cube.dev/track")
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

pub async fn track_event(event: String, properties: HashMap<String, String>) {
    SENDER.track_event(event, properties).await;
}

pub fn track_event_spawn(event: String, properties: HashMap<String, String>) {
    tokio::spawn(async move { SENDER.track_event(event, properties).await });
}

pub async fn start_track_event_loop() {
    let sender = SENDER.clone();
    tokio::spawn(async move { sender.send_loop().await });
}

pub async fn stop_track_event_loop() {
    SENDER.stop_loop().await;
}

pub struct ReportingLogger {
    logger: Box<dyn Log>,
}

impl ReportingLogger {
    pub fn init(logger: Box<dyn Log>, max_level: LevelFilter) -> Result<(), CubeError> {
        let reporting_logger = Self { logger };
        log::set_boxed_logger(Box::new(reporting_logger))?;
        log::set_max_level(max_level);
        Ok(())
    }
}

impl Log for ReportingLogger {
    fn enabled(&self, metadata: &Metadata<'a>) -> bool {
        self.logger.enabled(metadata)
    }

    fn log(&self, record: &Record<'a>) {
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
