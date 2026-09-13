use std::future::Future;
use std::sync::Arc;

use neon::prelude::*;
use serde_json;

use crate::auth::NativeSQLAuthContext;
use crate::config::NodeCubeServices;
use crate::cubesql_utils::with_session;
use crate::tokio_runtime_node;
use crate::utils::NonDebugInRelease;
use cubesql::compile::ast_conv;
use cubesql::transport::TransportLoadRequestQueryFilterItem;
use cubesql::CubeError;

use crate::rest4sql::json_value_to_js;

#[derive(Debug)]
enum SqlFiltersResponse {
    Ok {
        sql: Option<String>,
        filters: Vec<TransportLoadRequestQueryFilterItem>,
    },
    Error {
        error: String,
    },
}

impl SqlFiltersResponse {
    pub fn to_js<'ctx>(&self, cx: &mut impl Context<'ctx>) -> JsResult<'ctx, JsObject> {
        let obj = cx.empty_object();

        match &self {
            SqlFiltersResponse::Ok { sql, filters } => {
                let status = cx.string("ok");
                obj.set(cx, "status", status)?;

                if let Some(sql) = sql {
                    let sql = cx.string(sql);
                    obj.set(cx, "sql", sql)?;
                }

                let filters_json = serde_json::to_value(filters)
                    .or_else(|e| cx.throw_error(format!("Failed to serialize filters: {}", e)))?;
                let filters_js = json_value_to_js(cx, &filters_json)?;
                obj.set(cx, "filters", filters_js)?;
            }
            SqlFiltersResponse::Error { error } => {
                let status = cx.string("error");
                obj.set(cx, "status", status)?;

                let error = cx.string(error);
                obj.set(cx, "error", error)?;
            }
        }

        Ok(obj)
    }
}

/// Malformed filters are a caller mistake, so they are reported in-band as an
/// error response like any other bad input, rather than thrown as a JS error
/// (which the API gateway can't tell apart from an internal failure).
fn parse_filters_arg(
    cx: &mut FunctionContext,
    index: usize,
    what: &str,
) -> NeonResult<Result<Vec<TransportLoadRequestQueryFilterItem>, String>> {
    let filters_json = cx.argument::<JsString>(index)?.value(cx);
    Ok(serde_json::from_str(&filters_json).map_err(|e| format!("Failed to parse {}: {}", what, e)))
}

/// Returns a promise already resolved with an error response.
fn resolved_sql_filters_error<'a>(
    cx: &mut FunctionContext<'a>,
    error: String,
) -> JsResult<'a, JsValue> {
    let response = SqlFiltersResponse::Error { error }.to_js(cx)?;
    let (deferred, promise) = cx.promise();
    deferred.resolve(cx, response);

    Ok(promise.upcast::<JsValue>())
}

fn parse_security_context_arg(cx: &mut FunctionContext, index: usize) -> Arc<NativeSQLAuthContext> {
    let security_context: Option<serde_json::Value> = match cx.argument::<JsValue>(index) {
        Ok(string) => match string.downcast::<JsString, _>(cx) {
            Ok(v) => v.value(cx).parse::<serde_json::Value>().ok(),
            Err(_) => None,
        },
        Err(_) => None,
    };

    Arc::new(NativeSQLAuthContext {
        user: Some(String::from("unknown")),
        superuser: false,
        security_context: NonDebugInRelease::from(security_context),
    })
}

fn spawn_sql_filters_task<'a, Fut>(cx: &mut FunctionContext<'a>, task: Fut) -> JsResult<'a, JsValue>
where
    Fut: Future<Output = Result<SqlFiltersResponse, CubeError>> + Send + 'static,
{
    let runtime = tokio_runtime_node(cx)?;
    let channel = cx.channel();
    let (deferred, promise) = cx.promise();

    // Note: if the spawned task panics or is aborted before settling,
    // Neon's Drop implementation for Deferred automatically rejects the promise on the JS side.
    runtime.spawn(async move {
        let result = task.await;

        if let Err(err) = deferred.try_settle_with(&channel, move |mut cx| {
            // `neon::result::ResultExt` is implemented only for Result<Handle, Handle>, even though Ok variant is not touched
            let response = result.or_else(|err| cx.throw_error(err.to_string()))?;
            let response = response.to_js(&mut cx)?;
            Ok(response)
        }) {
            // There is not much we can do at this point
            // TODO lift this error to task => JoinHandle => JS watchdog
            log::error!(
                "Unable to settle JS promise from tokio task, try_settle_with failed, err: {err}"
            );
        }
    });

    Ok(promise.upcast::<JsValue>())
}

async fn handle_get_sql_filters(
    services: Arc<NodeCubeServices>,
    native_auth_ctx: Arc<NativeSQLAuthContext>,
    sql_query: String,
) -> Result<SqlFiltersResponse, CubeError> {
    with_session(&services, native_auth_ctx.clone(), |session| async move {
        let transport = session.server.transport.clone();
        let meta_context = transport.meta(native_auth_ctx).await?;

        match ast_conv::get_sql_filters(&sql_query, meta_context, session).await {
            Ok(filters) => Ok(SqlFiltersResponse::Ok { sql: None, filters }),
            Err(err) => Ok(SqlFiltersResponse::Error { error: err.message }),
        }
    })
    .await
}

async fn handle_add_sql_filters(
    services: Arc<NodeCubeServices>,
    native_auth_ctx: Arc<NativeSQLAuthContext>,
    sql_query: String,
    filters: Vec<TransportLoadRequestQueryFilterItem>,
) -> Result<SqlFiltersResponse, CubeError> {
    with_session(&services, native_auth_ctx.clone(), |session| async move {
        let transport = session.server.transport.clone();
        let meta_context = transport.meta(native_auth_ctx).await?;

        match ast_conv::add_sql_filters(&sql_query, &filters, meta_context, session).await {
            Ok(result) => Ok(SqlFiltersResponse::Ok {
                sql: Some(result.sql),
                filters: result.filters,
            }),
            Err(err) => Ok(SqlFiltersResponse::Error { error: err.message }),
        }
    })
    .await
}

async fn handle_set_sql_filters(
    services: Arc<NodeCubeServices>,
    native_auth_ctx: Arc<NativeSQLAuthContext>,
    sql_query: String,
    filters: Vec<TransportLoadRequestQueryFilterItem>,
) -> Result<SqlFiltersResponse, CubeError> {
    with_session(&services, native_auth_ctx.clone(), |session| async move {
        let transport = session.server.transport.clone();
        let meta_context = transport.meta(native_auth_ctx).await?;

        match ast_conv::set_sql_filters(&sql_query, &filters, meta_context, session).await {
            Ok(result) => Ok(SqlFiltersResponse::Ok {
                sql: Some(result.sql),
                filters: result.filters,
            }),
            Err(err) => Ok(SqlFiltersResponse::Error { error: err.message }),
        }
    })
    .await
}

async fn handle_delete_sql_filters(
    services: Arc<NodeCubeServices>,
    native_auth_ctx: Arc<NativeSQLAuthContext>,
    sql_query: String,
    filters: Vec<TransportLoadRequestQueryFilterItem>,
) -> Result<SqlFiltersResponse, CubeError> {
    with_session(&services, native_auth_ctx.clone(), |session| async move {
        let transport = session.server.transport.clone();
        let meta_context = transport.meta(native_auth_ctx).await?;

        match ast_conv::delete_sql_filters(&sql_query, &filters, meta_context, session).await {
            Ok(result) => Ok(SqlFiltersResponse::Ok {
                sql: Some(result.sql),
                filters: result.filters,
            }),
            Err(err) => Ok(SqlFiltersResponse::Error { error: err.message }),
        }
    })
    .await
}

async fn handle_replace_sql_filters(
    services: Arc<NodeCubeServices>,
    native_auth_ctx: Arc<NativeSQLAuthContext>,
    sql_query: String,
    old_filters: Vec<TransportLoadRequestQueryFilterItem>,
    new_filters: Vec<TransportLoadRequestQueryFilterItem>,
) -> Result<SqlFiltersResponse, CubeError> {
    with_session(&services, native_auth_ctx.clone(), |session| async move {
        let transport = session.server.transport.clone();
        let meta_context = transport.meta(native_auth_ctx).await?;

        match ast_conv::replace_sql_filters(
            &sql_query,
            &old_filters,
            &new_filters,
            meta_context,
            session,
        )
        .await
        {
            Ok(result) => Ok(SqlFiltersResponse::Ok {
                sql: Some(result.sql),
                filters: result.filters,
            }),
            Err(err) => Ok(SqlFiltersResponse::Error { error: err.message }),
        }
    })
    .await
}

pub fn get_sql_filters(mut cx: FunctionContext) -> JsResult<JsValue> {
    let interface = cx.argument::<JsBox<crate::node_export::SQLInterface>>(0)?;
    let sql_query = cx.argument::<JsString>(1)?.value(&mut cx);
    let native_auth_ctx = parse_security_context_arg(&mut cx, 2);
    let services = interface.services.clone();

    spawn_sql_filters_task(
        &mut cx,
        handle_get_sql_filters(services, native_auth_ctx, sql_query),
    )
}

pub fn add_sql_filters(mut cx: FunctionContext) -> JsResult<JsValue> {
    let interface = cx.argument::<JsBox<crate::node_export::SQLInterface>>(0)?;
    let sql_query = cx.argument::<JsString>(1)?.value(&mut cx);
    let filters = match parse_filters_arg(&mut cx, 2, "filters")? {
        Ok(filters) => filters,
        Err(error) => return resolved_sql_filters_error(&mut cx, error),
    };
    let native_auth_ctx = parse_security_context_arg(&mut cx, 3);
    let services = interface.services.clone();

    spawn_sql_filters_task(
        &mut cx,
        handle_add_sql_filters(services, native_auth_ctx, sql_query, filters),
    )
}

pub fn set_sql_filters(mut cx: FunctionContext) -> JsResult<JsValue> {
    let interface = cx.argument::<JsBox<crate::node_export::SQLInterface>>(0)?;
    let sql_query = cx.argument::<JsString>(1)?.value(&mut cx);
    let filters = match parse_filters_arg(&mut cx, 2, "filters")? {
        Ok(filters) => filters,
        Err(error) => return resolved_sql_filters_error(&mut cx, error),
    };
    let native_auth_ctx = parse_security_context_arg(&mut cx, 3);
    let services = interface.services.clone();

    spawn_sql_filters_task(
        &mut cx,
        handle_set_sql_filters(services, native_auth_ctx, sql_query, filters),
    )
}

pub fn delete_sql_filters(mut cx: FunctionContext) -> JsResult<JsValue> {
    let interface = cx.argument::<JsBox<crate::node_export::SQLInterface>>(0)?;
    let sql_query = cx.argument::<JsString>(1)?.value(&mut cx);
    let filters = match parse_filters_arg(&mut cx, 2, "filters")? {
        Ok(filters) => filters,
        Err(error) => return resolved_sql_filters_error(&mut cx, error),
    };
    let native_auth_ctx = parse_security_context_arg(&mut cx, 3);
    let services = interface.services.clone();

    spawn_sql_filters_task(
        &mut cx,
        handle_delete_sql_filters(services, native_auth_ctx, sql_query, filters),
    )
}

pub fn replace_sql_filters(mut cx: FunctionContext) -> JsResult<JsValue> {
    let interface = cx.argument::<JsBox<crate::node_export::SQLInterface>>(0)?;
    let sql_query = cx.argument::<JsString>(1)?.value(&mut cx);
    let old_filters = match parse_filters_arg(&mut cx, 2, "old filters")? {
        Ok(filters) => filters,
        Err(error) => return resolved_sql_filters_error(&mut cx, error),
    };
    let new_filters = match parse_filters_arg(&mut cx, 3, "new filters")? {
        Ok(filters) => filters,
        Err(error) => return resolved_sql_filters_error(&mut cx, error),
    };
    let native_auth_ctx = parse_security_context_arg(&mut cx, 4);
    let services = interface.services.clone();

    spawn_sql_filters_task(
        &mut cx,
        handle_replace_sql_filters(
            services,
            native_auth_ctx,
            sql_query,
            old_filters,
            new_filters,
        ),
    )
}
