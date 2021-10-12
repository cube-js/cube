use log::trace;
use neon::prelude::*;

use async_trait::async_trait;
use cubeclient::models::{V1LoadRequestQuery, V1LoadResponse, V1MetaResponse};
use cubesql::{
    compile::TenantContext, di_service, mysql::AuthContext, schema::SchemaService, CubeError,
};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::channel::{JsAsyncChannel, call_js_with_channel_as_callback};

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

#[async_trait]
impl SchemaService for NodeBridgeTransport {
    async fn get_ctx_for_tenant(&self, _ctx: &AuthContext) -> Result<TenantContext, CubeError> {
        trace!("[transport] Meta ->");

        let response: V1MetaResponse = call_js_with_channel_as_callback(self.channel.clone(), self.on_meta.clone(), None).await?;
        trace!("[transport] Meta <- {:?}", response);

        Ok(TenantContext {
            cubes: response.cubes.unwrap_or_default(),
        })
    }

    async fn request(
        &self,
        query: V1LoadRequestQuery,
        _ctx: &AuthContext,
    ) -> Result<V1LoadResponse, CubeError> {
        trace!("[transport] Request ->");

        let request = serde_json::to_string(&query)?;
        let response: V1LoadResponse = call_js_with_channel_as_callback(self.channel.clone(), self.on_load.clone(), Some(request))
            .await?;
        trace!("[transport] Request <- {:?}", response);

        Ok(response)
    }
}

di_service!(NodeBridgeTransport, [SchemaService]);
