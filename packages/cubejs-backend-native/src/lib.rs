#![feature(async_closure)]

mod auth;
mod channel;
mod config;
mod logger;
mod python;
mod stream;
mod transport;
mod utils;

use once_cell::sync::OnceCell;

use std::sync::Arc;

use crate::python::CubeConfigPy;
use auth::NodeBridgeAuthService;
use config::NodeConfig;
use cubesql::{config::CubeServices, telemetry::ReportingLogger};
use log::Level;
use logger::NodeBridgeLogger;
use neon::prelude::*;
use pyo3::prelude::*;
use simple_logger::SimpleLogger;
use tokio::runtime::{Builder, Runtime};
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

fn runtime<'a, C: Context<'a>>(cx: &mut C) -> NeonResult<&'static Runtime> {
    static RUNTIME: OnceCell<Runtime> = OnceCell::new();

    RUNTIME.get_or_try_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .or_else(|err| cx.throw_error(err.to_string()))
    })
}

fn py_runtime<'a, C: Context<'a>>(cx: &mut C) -> NeonResult<&()> {
    static PY_RUNTIME: OnceCell<()> = OnceCell::new();

    let runtime = runtime(cx)?;

    PY_RUNTIME.get_or_try_init(|| {
        pyo3::prepare_freethreaded_python();
        pyo3_asyncio::tokio::init_with_runtime(runtime).unwrap();

        Ok(())
    })
}

fn setup_logger(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let options = cx.argument::<JsObject>(0)?;
    let cube_logger = options
        .get::<JsFunction, _, _>(&mut cx, "logger")?
        .root(&mut cx);

    let log_level_handle = options.get_value(&mut cx, "logLevel")?;
    let log_level = if log_level_handle.is_a::<JsString, _>(&mut cx) {
        let value = log_level_handle.downcast_or_throw::<JsString, _>(&mut cx)?;
        let log_level = match value.value(&mut cx).as_str() {
            "error" => Level::Error,
            "warn" => Level::Warn,
            "info" => Level::Info,
            "debug" => Level::Debug,
            "trace" => Level::Trace,
            x => cx.throw_error(format!("Unrecognized log level: {}", x))?,
        };
        log_level
    } else {
        Level::Trace
    };

    let logger = SimpleLogger::new()
        .with_level(Level::Error.to_level_filter())
        .with_module_level("cubesql", log_level.to_level_filter())
        .with_module_level("cubejs_native", log_level.to_level_filter())
        .with_module_level("datafusion", Level::Warn.to_level_filter())
        .with_module_level("pg_srv", Level::Warn.to_level_filter());

    ReportingLogger::init(
        Box::new(logger),
        Box::new(NodeBridgeLogger::new(cx.channel(), cube_logger)),
        log_level.to_level_filter(),
    )
    .unwrap();

    Ok(cx.undefined())
}

fn register_interface(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let options = cx.argument::<JsObject>(0)?;
    let check_auth = options
        .get::<JsFunction, _, _>(&mut cx, "checkAuth")?
        .root(&mut cx);
    let transport_load = options
        .get::<JsFunction, _, _>(&mut cx, "load")?
        .root(&mut cx);
    let transport_meta = options
        .get::<JsFunction, _, _>(&mut cx, "meta")?
        .root(&mut cx);
    let transport_load_stream = options
        .get::<JsFunction, _, _>(&mut cx, "stream")?
        .root(&mut cx);

    let nonce_handle = options.get_value(&mut cx, "nonce")?;
    let nonce = if nonce_handle.is_a::<JsString, _>(&mut cx) {
        let value = nonce_handle.downcast_or_throw::<JsString, _>(&mut cx)?;
        Some(value.value(&mut cx))
    } else {
        None
    };

    let port_handle = options.get_value(&mut cx, "port")?;
    let port = if port_handle.is_a::<JsNumber, _>(&mut cx) {
        let value = port_handle.downcast_or_throw::<JsNumber, _>(&mut cx)?;

        Some(value.value(&mut cx) as u16)
    } else {
        None
    };

    let pg_port_handle = options.get_value(&mut cx, "pgPort")?;
    let pg_port = if pg_port_handle.is_a::<JsNumber, _>(&mut cx) {
        let value = pg_port_handle.downcast_or_throw::<JsNumber, _>(&mut cx)?;

        Some(value.value(&mut cx) as u16)
    } else {
        None
    };

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    let runtime = runtime(&mut cx)?;
    let transport_service = NodeBridgeTransport::new(
        cx.channel(),
        transport_load,
        transport_meta,
        transport_load_stream,
    );
    let auth_service = NodeBridgeAuthService::new(cx.channel(), check_auth);

    std::thread::spawn(move || {
        let config = NodeConfig::new(port, pg_port, nonce);

        runtime.block_on(async move {
            let services = Arc::new(
                config
                    .configure(Arc::new(transport_service), Arc::new(auth_service))
                    .await,
            );

            let services_arc = services.clone();
            let interface = SQLInterface::new(services_arc);

            log::debug!("Cube SQL Start");

            let mut loops = services.spawn_processing_loops().await.unwrap();
            loops.push(tokio::spawn(async move {
                deferred.settle_with(&channel, move |mut cx| Ok(cx.boxed(interface)));

                Ok(())
            }));

            CubeServices::wait_loops(loops).await.unwrap();
        });
    });

    Ok(promise)
}

fn shutdown_interface(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let interface = cx.argument::<JsBox<SQLInterface>>(0)?;

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    let services = interface.services.clone();
    let runtime = runtime(&mut cx)?;

    runtime.block_on(async move {
        let _ = services
            .stop_processing_loops()
            .await
            .or_else(|err| cx.throw_error(err.to_string()));
    });
    deferred.settle_with(&channel, move |mut cx| Ok(cx.undefined()));

    Ok(promise)
}

fn python_load_config(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let config_file_content = cx.argument::<JsString>(0)?.value(&mut cx);

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    py_runtime(&mut cx)?;

    let conf_res = Python::with_gil(|py| -> PyResult<CubeConfigPy> {
        let cube_conf_code = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/python/cube/src/conf/__init__.py"
        ));
        PyModule::from_code(py, cube_conf_code, "__init__.py", "cube.conf")?;

        let config_module = PyModule::from_code(py, &config_file_content, "config.py", "")?;
        let settings_py = config_module.getattr("settings")?;

        let mut cube_conf = CubeConfigPy::new();

        for attr_name in cube_conf.get_static_attrs() {
            cube_conf.static_from_attr(settings_py, attr_name)?;
        }

        cube_conf.apply_dynamic_functions(settings_py)?;

        Ok(cube_conf)
    });

    deferred.settle_with(&channel, move |mut cx| match conf_res {
        Ok(c) => c.to_object(&mut cx),
        Err(err) => cx.throw_error(format!("Python error: {}", err)),
    });

    Ok(promise)
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("setupLogger", setup_logger)?;
    cx.export_function("registerInterface", register_interface)?;
    cx.export_function("shutdownInterface", shutdown_interface)?;

    cx.export_function("pythonLoadConfig", python_load_config)?;

    Ok(())
}
