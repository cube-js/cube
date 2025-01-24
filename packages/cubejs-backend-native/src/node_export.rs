use cubesql::compile::DatabaseProtocol;
use cubesql::compile::{convert_sql_to_cube_query, get_df_batches};
use cubesql::config::processing_loop::ShutdownMode;
use cubesql::config::ConfigObj;
use cubesql::sql::SessionManager;
use cubesql::transport::TransportService;
use futures::StreamExt;

use serde_json::Map;
use tokio::sync::Semaphore;

use crate::auth::{NativeAuthContext, NodeBridgeAuthService};
use crate::channel::call_js_fn;
use crate::config::{NodeConfiguration, NodeConfigurationFactoryOptions, NodeCubeServices};
use crate::cross::CLRepr;
use crate::logger::NodeBridgeLogger;
use crate::stream::OnDrainHandler;
use crate::tokio_runtime_node;
use crate::transport::NodeBridgeTransport;
use crate::utils::batch_to_rows;
use cubenativeutils::wrappers::neon::context::neon_run_with_guarded_lifetime;
use cubenativeutils::wrappers::neon::inner_types::NeonInnerTypes;
use cubenativeutils::wrappers::neon::object::NeonObject;
use cubenativeutils::wrappers::object_handle::NativeObjectHandle;
use cubenativeutils::wrappers::serializer::NativeDeserialize;
use cubenativeutils::wrappers::NativeContextHolder;
use cubesqlplanner::cube_bridge::base_query_options::NativeBaseQueryOptions;
use cubesqlplanner::planner::base_query::BaseQuery;
use std::net::SocketAddr;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;

use cubesql::{telemetry::ReportingLogger, CubeError};

use neon::prelude::*;

struct SQLInterface {
    services: Arc<NodeCubeServices>,
}

impl Finalize for SQLInterface {}

impl SQLInterface {
    pub fn new(services: Arc<NodeCubeServices>) -> Self {
        Self { services }
    }
}

fn register_interface<C: NodeConfiguration>(mut cx: FunctionContext) -> JsResult<JsPromise> {
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

    let pg_port_handle = options.get_value(&mut cx, "pgPort")?;
    let pg_port = if pg_port_handle.is_a::<JsNumber, _>(&mut cx) {
        let value = pg_port_handle.downcast_or_throw::<JsNumber, _>(&mut cx)?;

        Some(value.value(&mut cx) as u16)
    } else {
        None
    };

    let gateway_port = options.get_value(&mut cx, "gatewayPort")?;
    let gateway_port = if gateway_port.is_a::<JsNumber, _>(&mut cx) {
        let value = gateway_port.downcast_or_throw::<JsNumber, _>(&mut cx)?;

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
        let config = C::new(NodeConfigurationFactoryOptions {
            gateway_port,
            pg_port,
        });

        runtime.block_on(async move {
            let services = config
                .configure(Arc::new(transport_service), Arc::new(auth_service))
                .await;

            let interface = SQLInterface::new(services.clone());

            log::debug!("Cube SQL Start");

            let mut loops = services.spawn_processing_loops().await.unwrap();
            loops.push(tokio::spawn(async move {
                deferred.settle_with(&channel, move |mut cx| Ok(cx.boxed(interface)));

                Ok(())
            }));
        });
    });

    Ok(promise)
}

fn shutdown_interface(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let interface = cx.argument::<JsBox<SQLInterface>>(0)?;
    let js_shutdown_mode = cx.argument::<JsString>(1)?;
    let shutdown_mode = match js_shutdown_mode.value(&mut cx).as_str() {
        "fast" => ShutdownMode::Fast,
        "semifast" => ShutdownMode::SemiFast,
        "smart" => ShutdownMode::Smart,
        _ => {
            return cx.throw_range_error::<&str, Handle<JsPromise>>(
                "ShutdownMode param must be 'fast', 'semifast', or 'smart'",
            );
        }
    };

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    let services = interface.services.clone();
    let runtime = tokio_runtime_node(&mut cx)?;

    runtime.spawn(async move {
        match services.stop_processing_loops(shutdown_mode).await {
            Ok(_) => {
                if let Err(err) = services.await_processing_loops().await {
                    log::error!("Error during awaiting on shutdown: {}", err)
                }

                deferred
                    .settle_with(&channel, move |mut cx| Ok(cx.undefined()))
                    .await
                    .unwrap();
            }
            Err(err) => {
                channel.send(move |mut cx| {
                    let err = JsError::error(&mut cx, err.to_string()).unwrap();
                    deferred.reject(&mut cx, err);
                    Ok(())
                });
            }
        };
    });

    Ok(promise)
}

const CHUNK_DELIM: &str = "\n";

async fn handle_sql_query(
    services: Arc<NodeCubeServices>,
    native_auth_ctx: Arc<NativeAuthContext>,
    channel: Arc<Channel>,
    stream_methods: WritableStreamMethods,
    sql_query: &String,
) -> Result<(), CubeError> {
    let config = services
        .injector()
        .get_service_typed::<dyn ConfigObj>()
        .await;

    let transport_service = services
        .injector()
        .get_service_typed::<dyn TransportService>()
        .await;
    let session_manager = services
        .injector()
        .get_service_typed::<SessionManager>()
        .await;

    let (host, port) = match SocketAddr::from_str(
        &config
            .postgres_bind_address()
            .clone()
            .unwrap_or("127.0.0.1:15432".into()),
    ) {
        Ok(addr) => (addr.ip().to_string(), addr.port()),
        Err(e) => {
            return Err(CubeError::internal(format!(
                "Failed to parse postgres_bind_address: {}",
                e
            )))
        }
    };

    let session = session_manager
        .create_session(DatabaseProtocol::PostgreSQL, host, port, None)
        .await?;

    session
        .state
        .set_auth_context(Some(native_auth_ctx.clone()));

    // todo: can we use compiler_cache?
    let meta_context = transport_service
        .meta(native_auth_ctx)
        .await
        .map_err(|err| CubeError::internal(format!("Failed to get meta context: {}", err)))?;
    let query_plan = convert_sql_to_cube_query(sql_query, meta_context, session).await?;

    let mut stream = get_df_batches(&query_plan).await?;

    let semaphore = Arc::new(Semaphore::new(0));

    let drain_handler = OnDrainHandler::new(
        channel.clone(),
        stream_methods.stream.clone(),
        semaphore.clone(),
    );

    drain_handler.handle(stream_methods.on.clone()).await?;

    let mut is_first_batch = true;
    while let Some(batch) = stream.next().await {
        let (columns, data) = batch_to_rows(batch?)?;

        if is_first_batch {
            let mut schema = Map::new();
            schema.insert("schema".into(), columns);
            let columns = format!(
                "{}{}",
                serde_json::to_string(&serde_json::Value::Object(schema))?,
                CHUNK_DELIM
            );
            is_first_batch = false;

            call_js_fn(
                channel.clone(),
                stream_methods.write.clone(),
                Box::new(|cx| {
                    let arg = cx.string(columns).upcast::<JsValue>();

                    Ok(vec![arg.upcast::<JsValue>()])
                }),
                Box::new(|cx, v| match v.downcast_or_throw::<JsBoolean, _>(cx) {
                    Ok(v) => Ok(v.value(cx)),
                    Err(_) => Err(CubeError::internal(
                        "Failed to downcast write response".to_string(),
                    )),
                }),
                stream_methods.stream.clone(),
            )
            .await?;
        }

        let mut rows = Map::new();
        rows.insert("data".into(), serde_json::Value::Array(data));
        let data = format!("{}{}", serde_json::to_string(&rows)?, CHUNK_DELIM);
        let js_stream_write_fn = stream_methods.write.clone();

        let should_pause = !call_js_fn(
            channel.clone(),
            js_stream_write_fn,
            Box::new(|cx| {
                let arg = cx.string(data).upcast::<JsValue>();

                Ok(vec![arg.upcast::<JsValue>()])
            }),
            Box::new(|cx, v| match v.downcast_or_throw::<JsBoolean, _>(cx) {
                Ok(v) => Ok(v.value(cx)),
                Err(_) => Err(CubeError::internal(
                    "Failed to downcast write response".to_string(),
                )),
            }),
            stream_methods.stream.clone(),
        )
        .await?;

        if should_pause {
            let permit = semaphore.acquire().await.unwrap();
            permit.forget();
        }
    }

    Ok(())
}

struct WritableStreamMethods {
    stream: Arc<Root<JsObject>>,
    on: Arc<Root<JsFunction>>,
    write: Arc<Root<JsFunction>>,
}

fn exec_sql(mut cx: FunctionContext) -> JsResult<JsValue> {
    let interface = cx.argument::<JsBox<SQLInterface>>(0)?;
    let sql_query = cx.argument::<JsString>(1)?.value(&mut cx);
    let node_stream = cx
        .argument::<JsObject>(2)?
        .downcast_or_throw::<JsObject, _>(&mut cx)?;

    let security_context: Option<serde_json::Value> = match cx.argument::<JsValue>(3) {
        Ok(string) => match string.downcast::<JsString, _>(&mut cx) {
            Ok(v) => v.value(&mut cx).parse::<serde_json::Value>().ok(),
            Err(_) => None,
        },
        Err(_) => None,
    };

    let js_stream_on_fn = Arc::new(
        node_stream
            .get::<JsFunction, _, _>(&mut cx, "on")?
            .root(&mut cx),
    );
    let js_stream_write_fn = Arc::new(
        node_stream
            .get::<JsFunction, _, _>(&mut cx, "write")?
            .root(&mut cx),
    );
    let js_stream_end_fn = Arc::new(
        node_stream
            .get::<JsFunction, _, _>(&mut cx, "end")?
            .root(&mut cx),
    );
    let node_stream_root = cx
        .argument::<JsObject>(2)?
        .downcast_or_throw::<JsObject, _>(&mut cx)?
        .root(&mut cx);

    let services = interface.services.clone();
    let runtime = tokio_runtime_node(&mut cx)?;

    let channel = Arc::new(cx.channel());
    let node_stream_arc = Arc::new(node_stream_root);

    let native_auth_ctx = Arc::new(NativeAuthContext {
        user: Some(String::from("unknown")),
        superuser: false,
        security_context,
    });

    let (deferred, promise) = cx.promise();

    runtime.spawn(async move {
        let stream_methods = WritableStreamMethods {
            stream: node_stream_arc.clone(),
            on: js_stream_on_fn,
            write: js_stream_write_fn,
        };

        let result = handle_sql_query(
            services,
            native_auth_ctx,
            channel.clone(),
            stream_methods,
            &sql_query,
        )
        .await;

        let _ = channel.try_send(move |mut cx| {
            let method = match Arc::try_unwrap(js_stream_end_fn) {
                Ok(v) => v.into_inner(&mut cx),
                Err(v) => v.as_ref().to_inner(&mut cx),
            };
            let this = match Arc::try_unwrap(node_stream_arc) {
                Ok(v) => v.into_inner(&mut cx),
                Err(v) => v.as_ref().to_inner(&mut cx),
            };

            let args = match result {
                Ok(_) => vec![],
                Err(err) => {
                    let mut error_response = Map::new();
                    error_response.insert("error".into(), err.to_string().into());
                    let error_response = format!(
                        "{}{}",
                        serde_json::to_string(&serde_json::Value::Object(error_response))
                            .expect("Failed to serialize error response to JSON"),
                        CHUNK_DELIM
                    );
                    let arg = cx.string(error_response).upcast::<JsValue>();

                    vec![arg]
                }
            };

            method.call(&mut cx, this, args)?;

            Ok(())
        });

        deferred.settle_with(&channel, move |mut cx| Ok(cx.undefined()));
    });

    Ok(promise.upcast::<JsValue>())
}

fn is_fallback_build(mut cx: FunctionContext) -> JsResult<JsBoolean> {
    #[cfg(feature = "python")]
    {
        return Ok(JsBoolean::new(&mut cx, false));
    }

    #[allow(unreachable_code)]
    Ok(JsBoolean::new(&mut cx, true))
}

pub fn setup_logger(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let options = cx.argument::<JsObject>(0)?;
    let cube_logger = options
        .get::<JsFunction, _, _>(&mut cx, "logger")?
        .root(&mut cx);

    let log_level_handle = options.get_value(&mut cx, "logLevel")?;
    let log_level = if log_level_handle.is_a::<JsString, _>(&mut cx) {
        let value = log_level_handle.downcast_or_throw::<JsString, _>(&mut cx)?;
        let log_level = match value.value(&mut cx).as_str() {
            "error" => log::Level::Error,
            "warn" => log::Level::Warn,
            "info" => log::Level::Info,
            "debug" => log::Level::Debug,
            "trace" => log::Level::Trace,
            x => cx.throw_error(format!("Unrecognized log level: {}", x))?,
        };
        log_level
    } else {
        log::Level::Trace
    };

    let logger = crate::create_logger(log_level);
    log_reroute::reroute_boxed(Box::new(logger));

    ReportingLogger::init(
        Box::new(NodeBridgeLogger::new(cx.channel(), cube_logger)),
        log_level.to_level_filter(),
    )
    .unwrap();

    Ok(cx.undefined())
}

//============ sql planner ===================

fn build_sql_and_params(cx: FunctionContext) -> JsResult<JsValue> {
    neon_run_with_guarded_lifetime(cx, |neon_context_holder| {
        let options =
            NativeObjectHandle::<NeonInnerTypes<FunctionContext<'static>>>::new(NeonObject::new(
                neon_context_holder.clone(),
                neon_context_holder
                    .with_context(|cx| cx.argument::<JsValue>(0))
                    .unwrap()?,
            ));

        let context_holder = NativeContextHolder::<NeonInnerTypes<FunctionContext<'static>>>::new(
            neon_context_holder,
        );

        let base_query_options = Rc::new(NativeBaseQueryOptions::from_native(options).unwrap());

        let base_query = BaseQuery::try_new(context_holder.clone(), base_query_options).unwrap();

        let res = base_query.build_sql_and_params();

        let result: NeonObject<FunctionContext<'static>> = res.into_object();
        let result = result.into_object();
        Ok(result)
    })
}

fn debug_js_to_clrepr_to_js(mut cx: FunctionContext) -> JsResult<JsValue> {
    let arg = cx.argument::<JsValue>(0)?;
    let arg_clrep = CLRepr::from_js_ref(arg, &mut cx)?;

    arg_clrep.into_js(&mut cx)
}

pub fn register_module_exports<C: NodeConfiguration + 'static>(
    mut cx: ModuleContext,
) -> NeonResult<()> {
    cx.export_function("setupLogger", setup_logger)?;
    cx.export_function("registerInterface", register_interface::<C>)?;
    cx.export_function("shutdownInterface", shutdown_interface)?;
    cx.export_function("execSql", exec_sql)?;
    cx.export_function("isFallbackBuild", is_fallback_build)?;
    cx.export_function("__js_to_clrepr_to_js", debug_js_to_clrepr_to_js)?;

    //============ sql planner exports ===================
    cx.export_function("buildSqlAndParams", build_sql_and_params)?;

    //========= sql orchestrator exports =================
    crate::orchestrator::register_module(&mut cx)?;

    crate::template::template_register_module(&mut cx)?;

    #[cfg(feature = "python")]
    crate::python::python_register_module(&mut cx)?;

    Ok(())
}
