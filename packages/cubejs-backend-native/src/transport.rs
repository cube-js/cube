use log::{debug, error, trace};
use neon::prelude::*;
use std::collections::HashMap;
use std::fmt::Display;

use async_trait::async_trait;
use cubeclient::models::{V1Error, V1LoadRequestQuery, V1LoadResponse, V1MetaResponse};
use cubesql::compile::engine::df::scan::{MemberField, SchemaRef};
use cubesql::compile::engine::df::wrapper::SqlQuery;
use cubesql::transport::{SpanId, SqlGenerator, SqlResponse};
use cubesql::{
    di_service,
    sql::AuthContextRef,
    transport::{CubeStreamReceiver, LoadRequestMeta, MetaContext, TransportService},
    CubeError,
};
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::NativeAuthContext;
use crate::channel::{call_raw_js_with_channel_as_callback, NodeSqlGenerator};
use crate::node_obj_serializer::NodeObjSerializer;
use crate::{
    auth::TransportRequest, channel::call_js_with_channel_as_callback,
    stream::call_js_with_stream_as_callback,
};

#[derive(Debug)]
pub struct NodeBridgeTransport {
    channel: Arc<Channel>,
    on_sql_api_load: Arc<Root<JsFunction>>,
    on_sql: Arc<Root<JsFunction>>,
    on_meta: Arc<Root<JsFunction>>,
    log_load_event: Arc<Root<JsFunction>>,
    sql_generators: Arc<Root<JsFunction>>,
    can_switch_user_for_session: Arc<Root<JsFunction>>,
}

impl NodeBridgeTransport {
    pub fn new(
        channel: Channel,
        on_sql_api_load: Root<JsFunction>,
        on_sql: Root<JsFunction>,
        on_meta: Root<JsFunction>,
        log_load_event: Root<JsFunction>,
        sql_generators: Root<JsFunction>,
        can_switch_user_for_session: Root<JsFunction>,
    ) -> Self {
        Self {
            channel: Arc::new(channel),
            on_sql_api_load: Arc::new(on_sql_api_load),
            on_sql: Arc::new(on_sql),
            on_meta: Arc::new(on_meta),
            log_load_event: Arc::new(log_load_event),
            sql_generators: Arc::new(sql_generators),
            can_switch_user_for_session: Arc::new(can_switch_user_for_session),
        }
    }
}

#[derive(Debug, Serialize)]
struct SessionContext {
    user: Option<String>,
    superuser: bool,
    #[serde(rename = "securityContext", skip_serializing_if = "Option::is_none")]
    security_context: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct CanSwitchUserForSessionRequest {
    session: SessionContext,
    user: String,
}

#[derive(Debug, Serialize)]
struct LoadRequest {
    request: TransportRequest,
    query: V1LoadRequestQuery,
    #[serde(rename = "sqlQuery", skip_serializing_if = "Option::is_none")]
    sql_query: Option<(String, Vec<Option<String>>)>,
    session: SessionContext,
    #[serde(rename = "memberToAlias", skip_serializing_if = "Option::is_none")]
    member_to_alias: Option<HashMap<String, String>>,
    #[serde(rename = "expressionParams", skip_serializing_if = "Option::is_none")]
    expression_params: Option<Vec<Option<String>>>,
    streaming: bool,
    #[serde(rename = "queryKey", skip_serializing_if = "Option::is_none")]
    query_key: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct LogEvent {
    request: TransportRequest,
    session: SessionContext,
    event: String,
    properties: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct MetaRequest {
    request: TransportRequest,
    session: SessionContext,
    #[serde(rename = "onlyCompilerId")]
    only_compiler_id: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SqlResponseSerialized {
    sql: (String, Vec<String>),
}

#[async_trait]
impl TransportService for NodeBridgeTransport {
    async fn meta(&self, ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
        trace!("[transport] Meta ->");

        let native_auth = ctx
            .as_any()
            .downcast_ref::<NativeAuthContext>()
            .expect("Unable to cast AuthContext to NativeAuthContext");

        let request_id = Uuid::new_v4().to_string();
        let extra = serde_json::to_string(&MetaRequest {
            request: TransportRequest {
                id: format!("{}-span-1", request_id),
                meta: None,
            },
            session: SessionContext {
                user: native_auth.user.clone(),
                superuser: native_auth.superuser,
                security_context: native_auth.security_context.clone(),
            },
            only_compiler_id: false,
        })?;

        let response = call_js_with_channel_as_callback::<V1MetaResponse>(
            self.channel.clone(),
            self.on_meta.clone(),
            Some(extra.clone()),
        )
        .await?;

        let channel = self.channel.clone();

        let (cube_to_data_source, data_source_to_sql_generator) =
            call_raw_js_with_channel_as_callback(
                self.channel.clone(),
                self.sql_generators.clone(),
                extra,
                Box::new(|cx, v| Ok(cx.string(v).as_value(cx))),
                Box::new(move |cx, v| {
                    let obj = v
                        .downcast::<JsObject, _>(cx)
                        .map_err(|e| CubeError::user(e.to_string()))?;
                    let cube_to_data_source_obj = obj
                        .get::<JsObject, _, _>(cx, "cubeNameToDataSource")
                        .map_cube_err("Can't cast cubeNameToDataSource to object")?;

                    let cube_to_data_source =
                        key_to_values(cx, cube_to_data_source_obj, |cx, v| {
                            let res = v.downcast::<JsString, _>(cx).map_cube_err(
                                "Can't cast value to string in cube_to_data_source",
                            )?;
                            Ok(res.value(cx))
                        })?;

                    let data_source_to_sql_generator_obj = obj
                        .get::<JsObject, _, _>(cx, "dataSourceToSqlGenerator")
                        .map_cube_err("Can't cast dataSourceToSqlGenerator to object")?;

                    let data_source_to_sql_generator =
                        key_to_values(cx, data_source_to_sql_generator_obj, move |cx, v| {
                            let sql_generator_obj = Arc::new(
                                v.downcast::<JsObject, _>(cx)
                                    .map_cube_err(
                                        "Can't cast dataSourceToSqlGenerator value to object",
                                    )?
                                    .root(cx),
                            );
                            let res: Arc<dyn SqlGenerator + Send + Sync> = Arc::new(
                                NodeSqlGenerator::new(cx, channel.clone(), sql_generator_obj)?,
                            );
                            Ok(res)
                        })?;

                    Ok((cube_to_data_source, data_source_to_sql_generator))
                }),
            )
            .await?;

        #[cfg(debug_assertions)]
        trace!(
            "[transport] Meta <- {:?} {:?}",
            response.compiler_id,
            response
        );
        #[cfg(not(debug_assertions))]
        trace!("[transport] Meta <- {:?} <hidden>", response.compiler_id);

        let compiler_id = Uuid::parse_str(response.compiler_id.as_ref().ok_or_else(|| {
            CubeError::user(format!("No compiler_id in response: {:?}", response))
        })?)
        .map_err(|e| {
            CubeError::user(format!(
                "Can't parse compiler id: {:?} error: {}",
                response.compiler_id, e
            ))
        })?;
        Ok(Arc::new(MetaContext::new(
            response.cubes.unwrap_or_default(),
            cube_to_data_source,
            data_source_to_sql_generator,
            compiler_id,
        )))
    }

    async fn compiler_id(&self, ctx: AuthContextRef) -> Result<Uuid, CubeError> {
        let native_auth = ctx
            .as_any()
            .downcast_ref::<NativeAuthContext>()
            .expect("Unable to cast AuthContext to NativeAuthContext");

        let request_id = Uuid::new_v4().to_string();
        let extra = serde_json::to_string(&MetaRequest {
            request: TransportRequest {
                id: format!("{}-span-1", request_id),
                meta: None,
            },
            session: SessionContext {
                user: native_auth.user.clone(),
                superuser: native_auth.superuser,
                security_context: native_auth.security_context.clone(),
            },
            only_compiler_id: true,
        })?;
        let response = call_js_with_channel_as_callback::<V1MetaResponse>(
            self.channel.clone(),
            self.on_meta.clone(),
            Some(extra.clone()),
        )
        .await?;

        let compiler_id = Uuid::parse_str(response.compiler_id.as_ref().ok_or_else(|| {
            CubeError::user(format!("No compiler_id in response: {:?}", response))
        })?)
        .map_err(|e| {
            CubeError::user(format!(
                "Can't parse compiler id: {:?} error: {}",
                response.compiler_id, e
            ))
        })?;
        Ok(compiler_id)
    }

    async fn sql(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: V1LoadRequestQuery,
        ctx: AuthContextRef,
        meta: LoadRequestMeta,
        member_to_alias: Option<HashMap<String, String>>,
        expression_params: Option<Vec<Option<String>>>,
    ) -> Result<SqlResponse, CubeError> {
        let native_auth = ctx
            .as_any()
            .downcast_ref::<NativeAuthContext>()
            .expect("Unable to cast AuthContext to NativeAuthContext");

        let request_id = span_id
            .as_ref()
            .map(|s| s.span_id.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let extra = serde_json::to_string(&LoadRequest {
            request: TransportRequest {
                id: format!("{}-span-{}", request_id, 1),
                meta: Some(meta.clone()),
            },
            query: query.clone(),
            query_key: span_id.map(|s| s.query_key.clone()),
            session: SessionContext {
                user: native_auth.user.clone(),
                superuser: native_auth.superuser,
                security_context: native_auth.security_context.clone(),
            },
            sql_query: None,
            member_to_alias,
            expression_params,
            streaming: false,
        })?;

        let response: serde_json::Value = call_js_with_channel_as_callback(
            self.channel.clone(),
            self.on_sql.clone(),
            Some(extra),
        )
        .await?;

        let sql = response
            .get("sql")
            .ok_or_else(|| CubeError::user(format!("No sql in response: {}", response)))?
            .get("sql")
            .ok_or_else(|| CubeError::user(format!("No sql in response: {}", response)))?;
        Ok(SqlResponse {
            sql: SqlQuery {
                sql: sql
                    .get(0)
                    .ok_or_else(|| {
                        CubeError::user(format!("No sql array in response: {}", response))
                    })?
                    .as_str()
                    .ok_or_else(|| {
                        CubeError::user(format!("SQL not a string in response: {}", response))
                    })?
                    .to_string(),
                values: sql
                    .get(1)
                    .ok_or_else(|| {
                        CubeError::user(format!("No sql array in response: {}", response))
                    })?
                    .as_array()
                    .ok_or_else(|| {
                        CubeError::user(format!("No sql array in response: {}", response))
                    })?
                    .iter()
                    .map(|v| -> Result<_, CubeError> { Ok(v.as_str().map(|s| s.to_string())) })
                    .collect::<Result<Vec<_>, _>>()?,
            },
        })
    }

    async fn load(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: V1LoadRequestQuery,
        sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta: LoadRequestMeta,
    ) -> Result<V1LoadResponse, CubeError> {
        trace!("[transport] Request ->");

        let native_auth = ctx
            .as_any()
            .downcast_ref::<NativeAuthContext>()
            .expect("Unable to cast AuthContext to NativeAuthContext");

        let request_id = span_id
            .as_ref()
            .map(|s| s.span_id.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        loop {
            let extra = serde_json::to_string(&LoadRequest {
                request: TransportRequest {
                    id: format!("{}-span-{}", request_id, 1),
                    meta: Some(meta.clone()),
                },
                query: query.clone(),
                query_key: span_id.as_ref().map(|s| s.query_key.clone()),
                session: SessionContext {
                    user: native_auth.user.clone(),
                    superuser: native_auth.superuser,
                    security_context: native_auth.security_context.clone(),
                },
                sql_query: sql_query.clone().map(|q| (q.sql, q.values)),
                member_to_alias: None,
                expression_params: None,
                streaming: false,
            })?;

            let result = call_js_with_channel_as_callback(
                self.channel.clone(),
                self.on_sql_api_load.clone(),
                Some(extra),
            )
            .await;
            if let Err(e) = &result {
                if e.message.to_lowercase().contains("continue wait") {
                    continue;
                }
            }
            let response: serde_json::Value = result?;
            #[cfg(debug_assertions)]
            trace!("[transport] Request <- {:?}", response);
            #[cfg(not(debug_assertions))]
            trace!("[transport] Request <- <hidden>");

            let load_err = match serde_json::from_value::<V1LoadResponse>(response.clone()) {
                Ok(r) => {
                    return Ok(r);
                }
                Err(err) => err,
            };

            if let Ok(res) = serde_json::from_value::<V1Error>(response) {
                if res.error.to_lowercase() == *"continue wait" {
                    debug!(
                        "[transport] load - retrying request (continue wait) requestId: {}",
                        request_id
                    );

                    continue;
                } else {
                    error!(
                        "[transport] load - strange response, success which contains error: {:?}",
                        res
                    );

                    return Err(CubeError::internal(res.error));
                }
            };

            return Err(CubeError::user(load_err.to_string()));
        }
    }

    async fn load_stream(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: V1LoadRequestQuery,
        sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta: LoadRequestMeta,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Result<CubeStreamReceiver, CubeError> {
        trace!("[transport] Request ->");

        let request_id = span_id
            .as_ref()
            .map(|s| s.span_id.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        loop {
            let native_auth = ctx
                .as_any()
                .downcast_ref::<NativeAuthContext>()
                .expect("Unable to cast AuthContext to NativeAuthContext");

            let extra = serde_json::to_string(&LoadRequest {
                request: TransportRequest {
                    id: format!("{}-span-{}", request_id, 1),
                    meta: Some(meta.clone()),
                },
                query: query.clone(),
                query_key: span_id.as_ref().map(|s| s.query_key.clone()),
                sql_query: sql_query.clone().map(|q| (q.sql, q.values)),
                session: SessionContext {
                    user: native_auth.user.clone(),
                    superuser: native_auth.superuser,
                    security_context: native_auth.security_context.clone(),
                },
                member_to_alias: None,
                expression_params: None,
                streaming: true,
            })?;

            let res = call_js_with_stream_as_callback(
                self.channel.clone(),
                self.on_sql_api_load.clone(),
                Some(extra),
                schema.clone(),
                member_fields.clone(),
            )
            .await;

            if let Err(e) = &res {
                if e.message.to_lowercase().contains("continue wait") {
                    continue;
                }
            }

            break res;
        }
    }

    async fn can_switch_user_for_session(
        &self,
        ctx: AuthContextRef,
        to_user: String,
    ) -> Result<bool, CubeError> {
        let native_auth = ctx
            .as_any()
            .downcast_ref::<NativeAuthContext>()
            .expect("Unable to cast AuthContext to NativeAuthContext");

        let res = call_raw_js_with_channel_as_callback(
            self.channel.clone(),
            self.can_switch_user_for_session.clone(),
            CanSwitchUserForSessionRequest {
                user: to_user,
                session: SessionContext {
                    user: native_auth.user.clone(),
                    superuser: native_auth.superuser,
                    security_context: native_auth.security_context.clone(),
                },
            },
            Box::new(|cx, v| match NodeObjSerializer::serialize(&v, cx) {
                Ok(res) => Ok(res),
                Err(e) => cx.throw_error(format!("Can't serialize to node obj: {}", e)),
            }),
            Box::new(move |cx, v| {
                let obj = v
                    .downcast::<JsBoolean, _>(cx)
                    .map_err(|e| CubeError::user(e.to_string()))?;
                Ok(obj.value(cx))
            }),
        )
        .await?;
        Ok(res)
    }

    async fn log_load_state(
        &self,
        span_id: Option<Arc<SpanId>>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        event: String,
        properties: serde_json::Value,
    ) -> Result<(), CubeError> {
        let native_auth = ctx
            .as_any()
            .downcast_ref::<NativeAuthContext>()
            .expect("Unable to cast AuthContext to NativeAuthContext");

        let request_id = span_id
            .map(|s| s.span_id.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        call_raw_js_with_channel_as_callback(
            self.channel.clone(),
            self.log_load_event.clone(),
            LogEvent {
                request: TransportRequest {
                    id: format!("{}-span-1", request_id),
                    meta: Some(meta_fields.clone()),
                },
                session: SessionContext {
                    user: native_auth.user.clone(),
                    superuser: native_auth.superuser,
                    security_context: native_auth.security_context.clone(),
                },
                event,
                properties,
            },
            Box::new(|cx, v| match NodeObjSerializer::serialize(&v, cx) {
                Ok(res) => Ok(res),
                Err(e) => cx.throw_error(format!("Can't serialize to node obj: {}", e)),
            }),
            Box::new(move |_, _| Ok(())),
        )
        .await
    }
}

// method to get keys to values using function from js object
fn key_to_values<T>(
    cx: &mut FunctionContext,
    obj: Handle<JsObject>,
    value_fn: impl Fn(&mut FunctionContext, Handle<JsValue>) -> Result<T, CubeError>,
) -> Result<HashMap<String, T>, CubeError> {
    let keys = obj
        .get_own_property_names(cx)
        .map_cube_err("Can't get property names in key_to_values")?;
    let mut values = HashMap::new();
    for i in 0..keys.len(cx) {
        let key = keys
            .get::<JsString, _, _>(cx, i)
            .map_cube_err("Can't cast key to string in key_to_values")?;
        let key = key.value(cx);
        let result = obj
            .get::<JsValue, _, _>(cx, key.as_str())
            .map_cube_err("Can't cast value to any in key_to_values")?;
        let value = value_fn(cx, result)?;
        values.insert(key, value);
    }
    Ok(values)
}

di_service!(NodeBridgeTransport, [TransportService]);

// Extension trait to map abstract errors to CubeError
pub trait MapCubeErrExt<T> {
    fn map_cube_err(self, message: &str) -> Result<T, CubeError>;
}

impl<T, E: Display> MapCubeErrExt<T> for Result<T, E> {
    fn map_cube_err(self, message: &str) -> Result<T, CubeError> {
        self.map_err(|e| CubeError::user(format!("{}: {}", message, e)))
    }
}
