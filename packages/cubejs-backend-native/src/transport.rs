use log::trace;
use neon::prelude::*;

use async_trait::async_trait;
use cubeclient::models::{V1LoadRequestQuery, V1LoadResponse, V1MetaResponse};
use cubesql::{
    compile::TenantContext, di_service, mysql::AuthContext, schema::SchemaService, CubeError,
};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::channel::JsAsyncChannel;

#[derive(Debug)]
pub struct NodeBridgeTransport {
    channel: Arc<Channel>,
    on_load: Arc<Root<JsFunction>>,
    on_meta: Arc<Root<JsFunction>>,
}

impl NodeBridgeTransport {
    pub fn new(
        channel: Channel,
        on_load: Root<JsFunction>,
        on_meta: Root<JsFunction>,
    ) -> NeonResult<NodeBridgeTransport> {
        Ok(NodeBridgeTransport {
            channel: Arc::new(channel),
            on_load: Arc::new(on_load),
            on_meta: Arc::new(on_meta),
        })
    }

    async fn send_request<R>(
        &self,
        js_method: Arc<Root<JsFunction>>,
        query: Option<String>,
    ) -> Result<R, CubeError>
    where
        R: 'static + serde::de::DeserializeOwned + Send + std::fmt::Debug,
    {
        let channel = self.channel.clone();

        let (tx, rx) = oneshot::channel::<Result<R, CubeError>>();
        let tx_mutex = Arc::new(Mutex::new(Some(tx)));

        let async_channel = JsAsyncChannel::new(Box::new(move |result| {
            let to_channel = match result {
                // @todo Optimize? Into?
                Ok(buffer_as_str) => match serde_json::from_str::<R>(&buffer_as_str) {
                    Ok(json) => Ok(json),
                    Err(err) => Err(CubeError::from_error(err)),
                },
                Err(err) => Err(CubeError::internal(err.to_string())),
            };

            if let Some(tx) = tx_mutex.lock().unwrap().take() {
                tx.send(to_channel).unwrap();
            } else {
                panic!("Resolve/Reject was called on AsyncChannel that was already resolved");
            }
        }));

        channel.send(move |mut cx| {
            // https://github.com/neon-bindings/neon/issues/672
            let method = match Arc::try_unwrap(js_method) {
                Ok(v) => v.into_inner(&mut cx),
                Err(v) => v.as_ref().to_inner(&mut cx),
            };

            let this = cx.undefined();
            let args: Vec<Handle<JsValue>> = vec![
                if let Some(q) = query {
                    cx.string(q).upcast::<JsValue>()
                } else {
                    cx.null().upcast::<JsValue>()
                },
                cx.boxed(async_channel).upcast::<JsValue>(),
            ];

            method.call(&mut cx, this, args)?;

            Ok(())
        });

        rx.await?
    }
}

#[async_trait]
impl SchemaService for NodeBridgeTransport {
    async fn get_ctx_for_tenant(&self, _ctx: &AuthContext) -> Result<TenantContext, CubeError> {
        trace!("[transport] Meta ->");

        let response: V1MetaResponse = self.send_request(self.on_meta.clone(), None).await?;
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
        let response: V1LoadResponse = self
            .send_request(self.on_load.clone(), Some(request))
            .await?;
        trace!("[transport] Request <- {:?}", response);

        Ok(response)
    }
}

di_service!(NodeBridgeTransport, [SchemaService]);
