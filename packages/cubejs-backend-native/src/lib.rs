#![feature(async_closure)]
#![feature(raw)]

mod auth;
mod channel;
mod config;
mod transport;

use std::{collections::HashMap, sync::Arc};

use channel::{channel_reject, channel_resolve};
use config::NodeConfig;
use cubesql::telemetry::track_event;
use neon::prelude::*;
use tokio::runtime::Builder;
use transport::NodeBridgeTransport;

struct SQLInterface {}

impl Finalize for SQLInterface {}

impl SQLInterface {}

fn register_interface(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let transport_load = cx.argument::<JsFunction>(0)?.root(&mut cx);
    let transport_meta = cx.argument::<JsFunction>(1)?.root(&mut cx);

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    let transport = NodeBridgeTransport::new(cx.channel(), transport_load, transport_meta)?;

    std::thread::spawn(move || {
        let config = NodeConfig::new();
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        // @todo await real?
        channel.settle_with(deferred, move |cx| Ok(cx.undefined()));

        runtime.block_on(async move {
            let services = config.configure(Arc::new(transport)).await;
            track_event("Cube SQL Start".to_string(), HashMap::new()).await;
            services.wait_processing_loops().await.unwrap();
        });
    });

    Ok(promise)
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("registerInterface", register_interface)?;
    cx.export_function("channel_resolve", channel_resolve)?;
    cx.export_function("channel_reject", channel_reject)?;

    Ok(())
}
