use log::{debug, error, trace};
use neon::prelude::*;

use async_trait::async_trait;
use cubeclient::models::{V1LoadConinueWait, V1LoadRequestQuery, V1LoadResponse, V1MetaResponse};
use cubesql::{
    compile::TenantContext, di_service, mysql::AuthContext, schema::SchemaService, CubeError,
};
use serde_derive::Serialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::channel::call_js_with_channel_as_callback;

#[derive(Debug)]
pub struct NodeBridgeTransport {
    channel: Arc<Channel>,
    on_load: Arc<Root<JsFunction>>,
    on_meta: Arc<Root<JsFunction>>,
}

impl NodeBridgeTransport {
    pub fn new(channel: Channel, on_load: Root<JsFunction>, on_meta: Root<JsFunction>) -> Self {
        Self {
            channel: Arc::new(channel),
            on_load: Arc::new(on_load),
            on_meta: Arc::new(on_meta),
        }
    }
}

#[derive(Debug, Serialize)]
struct LoadRequest {
    authorization: String,
    request_id: String,
    query: V1LoadRequestQuery,
}

#[derive(Debug, Serialize)]
struct MetaRequest {
    authorization: String,
}

#[async_trait]
impl SchemaService for NodeBridgeTransport {
    async fn get_ctx_for_tenant(&self, ctx: &AuthContext) -> Result<TenantContext, CubeError> {
        trace!("[transport] Meta ->");

        let extra = serde_json::to_string(&MetaRequest {
            authorization: ctx.access_token.clone(),
        })?;
        let response = call_js_with_channel_as_callback::<V1MetaResponse>(
            self.channel.clone(),
            self.on_meta.clone(),
            Some(extra),
        )
        .await?;
        trace!("[transport] Meta <- {:?}", response);

        Ok(TenantContext {
            cubes: response.cubes.unwrap_or_default(),
        })
    }

    async fn request(
        &self,
        query: V1LoadRequestQuery,
        ctx: &AuthContext,
    ) -> Result<V1LoadResponse, CubeError> {
        trace!("[transport] Request ->");

        let request_id = Uuid::new_v4().to_string();
        let mut span_counter: u32 = 1;

        loop {
            let extra = serde_json::to_string(&LoadRequest {
                authorization: ctx.access_token.clone(),
                request_id: format!("{}-span-{}", request_id, span_counter),
                query: query.clone(),
            })?;
            let response: serde_json::Value = call_js_with_channel_as_callback(
                self.channel.clone(),
                self.on_load.clone(),
                Some(extra),
            )
            .await?;
            trace!("[transport] Request <- {:?}", response);

            let load_err = match serde_json::from_value::<V1LoadResponse>(response.clone()) {
                Ok(r) => {
                    return Ok(r);
                }
                Err(err) => err,
            };

            if let Ok(res) = serde_json::from_value::<V1LoadConinueWait>(response) {
                if res.error.to_lowercase() == "continue wait".to_string() {
                    debug!(
                        "[transport] load - retrying request (continue wait) requestId: {}, span: {}",
                        request_id, span_counter
                    );

                    span_counter = span_counter + 1;

                    continue;
                } else {
                    error!(
                        "[transport] load - strange response, success which contains error: {:?}",
                        res
                    );

                    return Err(CubeError::internal(load_err.to_string()));
                }
            };

            return Err(CubeError::user(load_err.to_string()));
        }
    }
}

di_service!(NodeBridgeTransport, [SchemaService]);
