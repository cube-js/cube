use log::trace;
use neon::prelude::*;

use async_trait::async_trait;
use cubeclient::models::{V1LoadRequestQuery, V1LoadResponse, V1MetaResponse};
use cubesql::{
    compile::TenantContext, di_service, mysql::AuthContext, schema::SchemaService, CubeError,
};
use serde_derive::Serialize;
use std::sync::Arc;

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

        let extra = serde_json::to_string(&LoadRequest {
            authorization: ctx.access_token.clone(),
            query: query.clone(),
        })?;
        let response = call_js_with_channel_as_callback::<V1LoadResponse>(
            self.channel.clone(),
            self.on_load.clone(),
            Some(extra),
        )
        .await?;
        trace!("[transport] Request <- {:?}", response);

        Ok(response)
    }
}

di_service!(NodeBridgeTransport, [SchemaService]);
