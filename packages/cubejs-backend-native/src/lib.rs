#![feature(async_closure)]
#![feature(raw)]

mod auth;
mod channel;
mod config;
mod transport;

use std::{collections::HashMap, sync::Arc};

use auth::NodeBridgeAuthService;
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
    let options = cx.argument::<JsObject>(0)?;
    let check_auth = options
        .get(&mut cx, "checkAuth")?
        .downcast_or_throw::<JsFunction, _>(&mut cx)?
        .root(&mut cx);
    let transport_load = options
        .get(&mut cx, "load")?
        .downcast_or_throw::<JsFunction, _>(&mut cx)?
        .root(&mut cx);
    let transport_meta = options
        .get(&mut cx, "meta")?
        .downcast_or_throw::<JsFunction, _>(&mut cx)?
        .root(&mut cx);

    let port = options.get(&mut cx, "port")?;
    let configuration_port = if port.is_a::<JsNumber, _>(&mut cx) {
        let value = port.downcast_or_throw::<JsNumber, _>(&mut cx)?;
        let port = value.value(&mut cx) as u16;

        Some(port)
    } else {
        None
    };

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    let transport_service = NodeBridgeTransport::new(cx.channel(), transport_load, transport_meta);
    let auth_service = NodeBridgeAuthService::new(cx.channel(), check_auth);

    std::thread::spawn(move || {
        let config = NodeConfig::new(configuration_port);

        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        // @todo await real?
        channel.settle_with(deferred, move |cx| Ok(cx.undefined()));

        runtime.block_on(async move {
            let services = config
                .configure(Arc::new(transport_service), Arc::new(auth_service))
                .await;
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
