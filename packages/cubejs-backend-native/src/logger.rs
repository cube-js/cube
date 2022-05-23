use cubesql::telemetry::LogReporter;
use log::Level;
use neon::prelude::*;
use serde_derive::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::spawn;

use crate::channel::call_js_with_channel_as_callback;

#[derive(Debug)]
pub struct NodeBridgeLogger {
    channel: Arc<Channel>,
    on_track: Arc<Root<JsFunction>>,
}

impl NodeBridgeLogger {
    pub fn new(channel: Channel, on_track: Root<JsFunction>) -> Self {
        Self {
            channel: Arc::new(channel),
            on_track: Arc::new(on_track),
        }
    }
}

#[derive(Debug, Serialize)]
struct EventBox {
    event: HashMap<String, String>,
}

impl LogReporter for NodeBridgeLogger {
    fn log(&self, event: String, properties: HashMap<String, String>, _level: Level) {
        let mut props = properties;
        props.insert("type".to_string(), event);
        let extra = serde_json::to_string(&EventBox { event: props }).unwrap();

        let channel = self.channel.clone();
        let on_track = self.on_track.clone();
        // TODO: Move to spawning loops
        spawn(async move { log(channel, on_track, Some(extra)).await });
    }
}

async fn log(channel: Arc<Channel>, on_track: Arc<Root<JsFunction>>, extra: Option<String>) {
    let _ = call_js_with_channel_as_callback::<String>(channel, on_track, extra).await;
}
