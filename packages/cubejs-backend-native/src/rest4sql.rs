use std::sync::Arc;

use neon::prelude::*;
use serde_json;

use crate::auth::NativeSQLAuthContext;
use crate::config::NodeCubeServices;
use crate::cubesql_utils::with_session;
use crate::tokio_runtime_node;
use crate::utils::NonDebugInRelease;
use cubesql::compile::convert_sql_to_cube_query;
use cubesql::compile::datafusion::logical_plan::LogicalPlan;
use cubesql::compile::engine::df::scan::CubeScanNode;
use cubesql::transport::TransportLoadRequestQuery;
use cubesql::CubeError;

fn json_value_to_js<'ctx>(
    cx: &mut impl Context<'ctx>,
    value: &serde_json::Value,
) -> JsResult<'ctx, JsValue> {
    match value {
        serde_json::Value::Null => Ok(cx.null().upcast()),
        serde_json::Value::Bool(b) => Ok(cx.boolean(*b).upcast()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(cx.number(i as f64).upcast())
            } else if let Some(f) = n.as_f64() {
                Ok(cx.number(f).upcast())
            } else {
                cx.throw_error("Number conversion failed")
            }
        }
        serde_json::Value::String(s) => Ok(cx.string(s).upcast()),
        serde_json::Value::Array(arr) => {
            let js_array = cx.empty_array();
            for (i, item) in arr.iter().enumerate() {
                let js_value = json_value_to_js(cx, item)?;
                js_array.set(cx, i as u32, js_value)?;
            }
            Ok(js_array.upcast())
        }
        serde_json::Value::Object(obj) => {
            let js_obj = cx.empty_object();
            for (key, val) in obj.iter() {
                let js_value = json_value_to_js(cx, val)?;
                js_obj.set(cx, key.as_str(), js_value)?;
            }
            Ok(js_obj.upcast())
        }
    }
}

#[derive(Debug)]
enum Rest4SqlResponse {
    Ok {
        status: String,
        query: Box<TransportLoadRequestQuery>,
    },
    Error {
        status: String,
        error: String,
    },
}

impl Rest4SqlResponse {
    pub fn to_js<'ctx>(&self, cx: &mut impl Context<'ctx>) -> JsResult<'ctx, JsObject> {
        let obj = cx.empty_object();

        match &self {
            Rest4SqlResponse::Ok { status, query } => {
                let status = cx.string(status);
                obj.set(cx, "status", status)?;

                let query_json = serde_json::to_value(query)
                    .or_else(|e| cx.throw_error(format!("Failed to serialize query: {}", e)))?;
                let query_js = json_value_to_js(cx, &query_json)?;
                obj.set(cx, "query", query_js)?;
            }
            Rest4SqlResponse::Error { error, status } => {
                let status = cx.string(status);
                obj.set(cx, "status", status)?;

                let error = cx.string(error);
                obj.set(cx, "error", error)?;
            }
        }

        Ok(obj)
    }
}

async fn handle_rest4sql_query(
    services: Arc<NodeCubeServices>,
    native_auth_ctx: Arc<NativeSQLAuthContext>,
    sql_query: &str,
) -> Result<Rest4SqlResponse, CubeError> {
    with_session(&services, native_auth_ctx.clone(), |session| async move {
        let transport = session.server.transport.clone();
        let meta_context = transport
            .meta(native_auth_ctx)
            .await
            .map_err(|err| CubeError::internal(format!("Failed to get meta context: {err}")))?;
        let query_plan =
            convert_sql_to_cube_query(sql_query, meta_context.clone(), session.clone()).await?;
        let logical_plan = query_plan.try_as_logical_plan()?;

        match logical_plan {
            LogicalPlan::Extension(extension) => {
                if let Some(cube_scan) = extension.node.as_any().downcast_ref::<CubeScanNode>() {
                    return Ok(Rest4SqlResponse::Ok {
                        status: "ok".to_string(),
                        query: Box::new(cube_scan.request.clone()),
                    });
                }

                Ok(Rest4SqlResponse::Error {
                    status: "error".to_string(),
                    error: "Provided sql query can not be converted to rest query.".to_string(),
                })
            }
            _ => Ok(Rest4SqlResponse::Error {
                status: "error".to_string(),
                error: "Provided sql query can not be converted to rest query.".to_string(),
            }),
        }
    })
    .await
}

pub fn rest4sql(mut cx: FunctionContext) -> JsResult<JsValue> {
    let interface = cx.argument::<JsBox<crate::node_export::SQLInterface>>(0)?;
    let sql_query = cx.argument::<JsString>(1)?.value(&mut cx);

    let security_context: Option<serde_json::Value> = match cx.argument::<JsValue>(2) {
        Ok(string) => match string.downcast::<JsString, _>(&mut cx) {
            Ok(v) => v.value(&mut cx).parse::<serde_json::Value>().ok(),
            Err(_) => None,
        },
        Err(_) => None,
    };

    let services = interface.services.clone();
    let runtime = tokio_runtime_node(&mut cx)?;

    let channel = cx.channel();

    let native_auth_ctx = Arc::new(NativeSQLAuthContext {
        user: Some(String::from("unknown")),
        superuser: false,
        security_context: NonDebugInRelease::from(security_context),
    });

    let (deferred, promise) = cx.promise();

    // In case spawned task panics or gets aborted before settle call it will leave permanently pending Promise in JS land
    // We don't want to just waste whole thread (doesn't really matter main or worker or libuv thread pool)
    // just busy waiting that JoinHandle
    // TODO handle JoinError
    //  keep JoinHandle alive in JS thread
    //  check join handle from JS thread periodically, reject promise on JoinError
    //  maybe register something like uv_check handle (libuv itself does not have ABI stability of N-API)
    //  can do it relatively rare, and in a single loop for all JoinHandles
    //  this is just a watchdog for a Very Bad case, so latency requirement can be quite relaxed
    runtime.spawn(async move {
        let result = handle_rest4sql_query(services, native_auth_ctx, &sql_query).await;

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
