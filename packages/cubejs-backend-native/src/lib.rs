#![feature(async_closure)]
#![feature(thread_id_value)]
#![allow(clippy::result_large_err)]

extern crate findshlibs;

mod auth;
mod channel;
mod config;
mod cross;
mod logger;
mod node_obj_serializer;
#[cfg(feature = "python")]
mod python;
mod stream;
mod template;
mod transport;
mod utils;

use channel::call_raw_js_with_channel_as_callback;
use cubesql::compile::{convert_sql_to_cube_query, get_df_batches, print_df_stream};
use cubesql::sql::{
    self, dataframe, AuthContext, DatabaseProtocol, HttpAuthContext, PostgresServer, SessionManager,
};
use cubesql::transport::TransportService;
use futures::StreamExt;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::channel::call_sync_js_fn;
use crate::cross::CLRepr;
use auth::{NativeAuthContext, NodeBridgeAuthService};
use config::NodeConfig;
use cubesql::telemetry::LocalReporter;
use cubesql::{config::CubeServices, telemetry::ReportingLogger, CubeError};
use log::Level;
use logger::NodeBridgeLogger;
use neon::prelude::*;
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

fn tokio_runtime_node<'a, C: Context<'a>>(cx: &mut C) -> NeonResult<&'static Runtime> {
    match tokio_runtime() {
        Ok(r) => Ok(r),
        Err(err) => cx.throw_error(err.to_string()),
    }
}

fn tokio_runtime() -> Result<&'static Runtime, CubeError> {
    static RUNTIME: OnceCell<Runtime> = OnceCell::new();

    RUNTIME.get_or_try_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| CubeError::internal(err.to_string()))
    })
}

fn create_logger(log_level: log::Level) -> SimpleLogger {
    SimpleLogger::new()
        .with_level(Level::Error.to_level_filter())
        .with_module_level("cubesql", log_level.to_level_filter())
        .with_module_level("cubejs_native", log_level.to_level_filter())
        .with_module_level("datafusion", Level::Warn.to_level_filter())
        .with_module_level("pg_srv", Level::Warn.to_level_filter())
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

    let logger = create_logger(log_level);
    log_reroute::reroute_boxed(Box::new(logger));

    ReportingLogger::init(
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
    let transport_sql_api_load = options
        .get::<JsFunction, _, _>(&mut cx, "sqlApiLoad")?
        .root(&mut cx);
    let transport_sql = options
        .get::<JsFunction, _, _>(&mut cx, "sql")?
        .root(&mut cx);
    let transport_meta = options
        .get::<JsFunction, _, _>(&mut cx, "meta")?
        .root(&mut cx);
    let transport_log_load_event = options
        .get::<JsFunction, _, _>(&mut cx, "logLoadEvent")?
        .root(&mut cx);
    let transport_sql_generator = options
        .get::<JsFunction, _, _>(&mut cx, "sqlGenerators")?
        .root(&mut cx);
    let transport_can_switch_user_for_session = options
        .get::<JsFunction, _, _>(&mut cx, "canSwitchUserForSession")?
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

    let runtime = tokio_runtime_node(&mut cx)?;
    let transport_service = NodeBridgeTransport::new(
        cx.channel(),
        transport_sql_api_load,
        transport_sql,
        transport_meta,
        transport_log_load_event,
        transport_sql_generator,
        transport_can_switch_user_for_session,
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
    let runtime = tokio_runtime_node(&mut cx)?;

    runtime.block_on(async move {
        let _ = services
            .stop_processing_loops()
            .await
            .or_else(|err| cx.throw_error(err.to_string()));
    });
    deferred.settle_with(&channel, move |mut cx| Ok(cx.undefined()));

    Ok(promise)
}

fn exec_sql(mut cx: FunctionContext) -> JsResult<JsValue> {
    let native_auth_ctx = Arc::new(NativeAuthContext {
        user: Some(String::from("cube")),
        superuser: false,
        security_context: Some(serde_json::json!({})),
    });

    let interface = cx.argument::<JsBox<SQLInterface>>(0)?;
    let sql_query = cx.argument::<JsString>(1)?.value(&mut cx);
    let node_stream = cx
        .argument::<JsObject>(2)?
        .downcast_or_throw::<JsObject, _>(&mut cx)?;
    let node_stream_root = cx
        .argument::<JsObject>(2)?
        .downcast_or_throw::<JsObject, _>(&mut cx)?
        .root(&mut cx);

    let services = interface.services.clone();
    let runtime = tokio_runtime_node(&mut cx)?;

    eprintln!("from query {:?}", sql_query);

    let js_push = Arc::new(
        node_stream
            .get::<JsFunction, _, _>(&mut cx, "push")?
            .root(&mut cx),
    );

    let channel = Arc::new(cx.channel());
    let node_stream_arc = Arc::new(node_stream_root);

    // runtime.spawn(async move {
    //     for i in 0..10 {
    //         eprintln!("Iteration {:?}", i);
    //         let argument = String::from(format!("hello_{}", i));
    //         call_sync_js_fn(
    //             channel.clone(),
    //             js_push.clone(),
    //             if i != 9 { Some(argument.clone()) } else { None },
    //             node_stream_arc.clone(),
    //         )
    //         .unwrap();

    //         tokio::time::sleep(Duration::from_secs(1)).await;
    //     }
    // });

    runtime.spawn(async move {
        let session_manager = services
            .injector
            .get_service_typed::<SessionManager>()
            .await;
        let session = session_manager
            .create_session(
                DatabaseProtocol::PostgreSQL,
                String::from("127.0.0.1"),
                15432,
            )
            .await;

        session
            .state
            .set_auth_context(Some(native_auth_ctx.clone()));

        let transport_service = services
            .injector
            .get_service_typed::<dyn TransportService>()
            .await;

        // todo: can we use compiler_cache?
        let meta_context = transport_service.meta(native_auth_ctx).await.unwrap();
        let query_plan = convert_sql_to_cube_query(&sql_query, meta_context, session)
            .await
            .unwrap();

        let mut stream = get_df_batches(&query_plan).await.unwrap();
        // let mut rows: Vec<HashMap<String, String>> = vec![];
        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => {
                    let schema = batch.schema();
                    let data_frame = dataframe::batch_to_dataframe(&schema, &vec![batch]).unwrap();

                    eprintln!("@batch columns ----> {:?}", data_frame.get_columns());
                    let rows = data_frame
                        .get_rows()
                        .iter()
                        .map(|it| {
                            let data = it
                                .values()
                                .iter()
                                .map(|it| it.to_string())
                                .collect::<Vec<String>>();

                            let json_data = serde_json::to_value(&data).unwrap();

                            call_sync_js_fn(
                                channel.clone(),
                                js_push.clone(),
                                Some(json_data.to_string()),
                                node_stream_arc.clone(),
                            )
                            .unwrap();

                            data
                        })
                        .collect::<Vec<Vec<String>>>();

                    eprintln!("@batch rows ----> {:?}", rows);
                }
                _ => todo!(),
            }
        }

        call_sync_js_fn(channel.clone(), js_push.clone(), None, node_stream_arc).unwrap();
        // print_df_stream(&query_plan).await.unwrap();

        // while Some(batch) = stream.next().await {
        //     let batch = batch.unwrap();
        //     let output = sql::serialize_batch(&batch).unwrap();
        // }
    });

    // Ok(promise.upcast::<JsValue>())
    Ok(JsUndefined::new(&mut cx).upcast::<JsValue>())
}

fn is_fallback_build(mut cx: FunctionContext) -> JsResult<JsBoolean> {
    #[cfg(feature = "python")]
    {
        return Ok(JsBoolean::new(&mut cx, false));
    }

    #[allow(unreachable_code)]
    Ok(JsBoolean::new(&mut cx, true))
}

fn debug_js_to_clrepr_to_js(mut cx: FunctionContext) -> JsResult<JsValue> {
    let arg = cx.argument::<JsValue>(1)?;
    let arg_clrep = CLRepr::from_js_ref(arg, &mut cx)?;

    arg_clrep.into_js(&mut cx)
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    // We use log_rerouter to swap logger, because we init logger from js side in api-gateway
    log_reroute::init().unwrap();

    let logger = Box::new(create_logger(Level::Error));
    log_reroute::reroute_boxed(logger);

    ReportingLogger::init(
        Box::new(LocalReporter::new()),
        Level::Error.to_level_filter(),
    )
    .unwrap();

    cx.export_function("setupLogger", setup_logger)?;
    cx.export_function("registerInterface", register_interface)?;
    cx.export_function("shutdownInterface", shutdown_interface)?;
    cx.export_function("execSql", exec_sql)?;
    cx.export_function("isFallbackBuild", is_fallback_build)?;
    cx.export_function("__js_to_clrepr_to_js", debug_js_to_clrepr_to_js)?;

    template::template_register_module(&mut cx)?;

    #[cfg(feature = "python")]
    python::python_register_module(&mut cx)?;

    Ok(())
}
