#![feature(async_closure)]
#![feature(raw)]

mod auth;
mod channel;
mod config;
mod transport;
mod utils;

use std::{collections::HashMap, sync::Arc};

use auth::NodeBridgeAuthService;
use config::NodeConfig;
use cubesql::{
    config::CubeServices,
    telemetry::{track_event, ReportingLogger},
};
use log::Level;
use neon::prelude::*;
use simple_logger::SimpleLogger;
use tokio::runtime::Builder;
use transport::NodeBridgeTransport;

struct SQLInterface {
    services: Arc<CubeServices>,
}

impl Finalize for SQLInterface {}

impl SQLInterface {
    pub fn new(services: Arc<CubeServices>) -> Self {
        Self { services }
    }
}

fn init_logger(log_level: Level) {
    let logger = SimpleLogger::new()
        .with_level(Level::Error.to_level_filter())
        .with_module_level("cubesql", log_level.to_level_filter())
        .with_module_level("cubejs_native", log_level.to_level_filter());

    ReportingLogger::init(Box::new(logger), log_level.to_level_filter()).unwrap();
}

fn set_log_level(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let log_level = match cx
        .argument::<JsString>(0)?
        .value(&mut cx)
        .to_lowercase()
        .as_str()
    {
        "error" => Level::Error,
        "warn" => Level::Warn,
        "info" => Level::Info,
        "debug" => Level::Debug,
        "trace" => Level::Trace,
        x => cx.throw_error(format!("Unrecognized log level: {}", x))?,
    };

    init_logger(log_level);

    Ok(cx.undefined())
}

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

    let nonce_handle = options.get(&mut cx, "nonce")?;
    let nonce = if nonce_handle.is_a::<JsString, _>(&mut cx) {
        let value = nonce_handle.downcast_or_throw::<JsString, _>(&mut cx)?;
        Some(value.value(&mut cx))
    } else {
        None
    };

    let port_handle = options.get(&mut cx, "port")?;
    let port = if port_handle.is_a::<JsNumber, _>(&mut cx) {
        let value = port_handle.downcast_or_throw::<JsNumber, _>(&mut cx)?;

        Some(value.value(&mut cx) as u16)
    } else {
        None
    };

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    let transport_service = NodeBridgeTransport::new(cx.channel(), transport_load, transport_meta);
    let auth_service = NodeBridgeAuthService::new(cx.channel(), check_auth);

    std::thread::spawn(move || {
        let config = NodeConfig::new(port, nonce);

        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        runtime.block_on(async move {
            let services = Arc::new(
                config
                    .configure(Arc::new(transport_service), Arc::new(auth_service))
                    .await,
            );

            let services_arc = services.clone();
            channel.settle_with(deferred, move |cx| {
                Ok(cx.boxed(SQLInterface::new(services_arc)))
            });

            track_event("Cube SQL Start".to_string(), HashMap::new()).await;
            services.wait_processing_loops().await.unwrap();
        });
    });

    Ok(promise)
}

fn shutdown_interface(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let interface = cx.argument::<JsBox<SQLInterface>>(0)?;

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    let services = interface.services.clone();
    // @todo Await without runtime?
    #[warn(unused_must_use)]
    services.stop_processing_loops();

    // @todo How to stop tokio runtime?

    channel.settle_with(deferred, move |cx| Ok(cx.undefined()));

    Ok(promise)
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("setLogLevel", set_log_level)?;
    cx.export_function("registerInterface", register_interface)?;
    cx.export_function("shutdownInterface", shutdown_interface)?;

    Ok(())
}
