use std::sync::Arc;

use neon::prelude::*;

use crate::auth::{parse_security_context_arg, NativeSQLAuthContext};
use crate::config::NodeCubeServices;
use crate::cubesql_utils::with_session;
use crate::rest4sql::json_value_to_js;
use crate::tokio_runtime_node;
use cubesql::compile::parameters::describe_parameters;
use cubesql::compile::parser::parse_sql_to_statement;
use cubesql::{CubeError, CubeErrorCauseType};

/// `{ params }`, or `{ error }` for the caller's failures, which the gateway maps to a 400;
/// internal ones are thrown, so they reach its error handler as a 500.
async fn handle_describe_sql_params(
    services: Arc<NodeCubeServices>,
    native_auth_ctx: Arc<NativeSQLAuthContext>,
    sql_query: &str,
) -> Result<serde_json::Value, CubeError> {
    let result = with_session(&services, native_auth_ctx.clone(), |session| async move {
        let transport = session.server.transport.clone();
        let meta_context = transport.meta(native_auth_ctx).await?;
        let stmt = parse_sql_to_statement(sql_query, session.state.protocol.clone(), &mut None)?;
        Ok(describe_parameters(&stmt, meta_context, session)?)
    })
    .await;

    match result {
        Ok(params) => Ok(serde_json::json!({ "params": params })),
        Err(err) => match err.cause {
            CubeErrorCauseType::Internal(_) => Err(err),
            _ => Ok(serde_json::json!({ "error": err.message })),
        },
    }
}

pub fn describe_sql_params(mut cx: FunctionContext) -> JsResult<JsValue> {
    let interface = cx.argument::<JsBox<crate::node_export::SQLInterface>>(0)?;
    let sql_query = cx.argument::<JsString>(1)?.value(&mut cx);

    let native_auth_ctx = parse_security_context_arg(&mut cx, 2)?;

    let services = interface.services.clone();
    let runtime = tokio_runtime_node(&mut cx)?;

    let channel = cx.channel();

    let (deferred, promise) = cx.promise();

    runtime.spawn(async move {
        let result = handle_describe_sql_params(services, native_auth_ctx, &sql_query).await;

        if let Err(err) = deferred.try_settle_with(&channel, move |mut cx| {
            let json = result.or_else(|err| cx.throw_error(err.to_string()))?;
            json_value_to_js(&mut cx, &json)
        }) {
            log::error!(
                "Unable to settle JS promise from tokio task, try_settle_with failed, err: {err}"
            );
        }
    });

    Ok(promise.upcast::<JsValue>())
}
