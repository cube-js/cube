use log::{debug, error, trace};
use neon::prelude::*;
use std::collections::HashMap;
use std::fmt::Display;

use async_trait::async_trait;
use cubeclient::models::{V1Error, V1LoadRequestQuery, V1LoadResponse, V1MetaResponse};
use cubesql::compile::engine::df::scan::{MemberField, SchemaRef};
use cubesql::transport::SqlGenerator;
use cubesql::{
    di_service,
    sql::AuthContextRef,
    transport::{CubeStreamReceiver, LoadRequestMeta, MetaContext, TransportService},
    CubeError,
};
use serde_derive::Serialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::NativeAuthContext;
use crate::channel::{call_raw_js_with_channel_as_callback, NodeSqlGenerator};
use crate::{
    auth::TransportRequest, channel::call_js_with_channel_as_callback,
    stream::call_js_with_stream_as_callback,
};

#[derive(Debug)]
pub struct NodeBridgeTransport {
    channel: Arc<Channel>,
    on_load: Arc<Root<JsFunction>>,
    on_meta: Arc<Root<JsFunction>>,
    on_load_stream: Arc<Root<JsFunction>>,
    sql_generators: Arc<Root<JsFunction>>,
}

impl NodeBridgeTransport {
    pub fn new(
        channel: Channel,
        on_load: Root<JsFunction>,
        on_meta: Root<JsFunction>,
        on_load_stream: Root<JsFunction>,
        sql_generators: Root<JsFunction>,
    ) -> Self {
        Self {
            channel: Arc::new(channel),
            on_load: Arc::new(on_load),
            on_meta: Arc::new(on_meta),
            on_load_stream: Arc::new(on_load_stream),
            sql_generators: Arc::new(sql_generators),
        }
    }
}

#[derive(Debug, Serialize)]
struct SessionContext {
    user: Option<String>,
    superuser: bool,
}

#[derive(Debug, Serialize)]
struct LoadRequest {
    request: TransportRequest,
    query: V1LoadRequestQuery,
    session: SessionContext,
}

#[derive(Debug, Serialize)]
struct MetaRequest {
    request: TransportRequest,
    session: SessionContext,
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
            },
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
                Box::new(|cx, v| cx.string(v).as_value(cx)),
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
        trace!("[transport] Meta <- {:?}", response);
        #[cfg(not(debug_assertions))]
        trace!("[transport] Meta <- <hidden>");

        Ok(Arc::new(MetaContext::new(
            response.cubes.unwrap_or_default(),
            cube_to_data_source,
            data_source_to_sql_generator,
        )))
    }

    async fn load(
        &self,
        query: V1LoadRequestQuery,
        ctx: AuthContextRef,
        meta: LoadRequestMeta,
    ) -> Result<V1LoadResponse, CubeError> {
        trace!("[transport] Request ->");

        let native_auth = ctx
            .as_any()
            .downcast_ref::<NativeAuthContext>()
            .expect("Unable to cast AuthContext to NativeAuthContext");

        let request_id = Uuid::new_v4().to_string();
        let mut span_counter: u32 = 1;

        loop {
            let extra = serde_json::to_string(&LoadRequest {
                request: TransportRequest {
                    id: format!("{}-span-{}", request_id, span_counter),
                    meta: Some(meta.clone()),
                },
                query: query.clone(),
                session: SessionContext {
                    user: native_auth.user.clone(),
                    superuser: native_auth.superuser,
                },
            })?;

            let response: serde_json::Value = call_js_with_channel_as_callback(
                self.channel.clone(),
                self.on_load.clone(),
                Some(extra),
            )
            .await?;
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
                        "[transport] load - retrying request (continue wait) requestId: {}, span: {}",
                        request_id, span_counter
                    );

                    span_counter += 1;

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
        query: V1LoadRequestQuery,
        ctx: AuthContextRef,
        meta: LoadRequestMeta,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Result<CubeStreamReceiver, CubeError> {
        trace!("[transport] Request ->");

        let request_id = Uuid::new_v4().to_string();
        let mut span_counter: u32 = 1;
        loop {
            let native_auth = ctx
                .as_any()
                .downcast_ref::<NativeAuthContext>()
                .expect("Unable to cast AuthContext to NativeAuthContext");

            let extra = serde_json::to_string(&LoadRequest {
                request: TransportRequest {
                    id: format!("{}-span-{}", request_id, span_counter),
                    meta: Some(meta.clone()),
                },
                query: query.clone(),
                session: SessionContext {
                    user: native_auth.user.clone(),
                    superuser: native_auth.superuser,
                },
            })?;

            let res = call_js_with_stream_as_callback(
                self.channel.clone(),
                self.on_load_stream.clone(),
                Some(extra),
                schema.clone(),
                member_fields.clone(),
            )
            .await;

            if let Err(e) = &res {
                if e.message.to_lowercase().contains("continue wait") {
                    span_counter += 1;
                    continue;
                }
            }

            break res;
        }
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
