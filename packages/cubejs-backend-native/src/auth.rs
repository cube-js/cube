use async_trait::async_trait;
use cubesql::{
    di_service,
    mysql::{AuthContext, SqlAuthService},
    CubeError,
};
use log::trace;
use neon::prelude::*;
use std::sync::Arc;

use crate::channel::call_js_with_channel_as_callback;

#[derive(Debug)]
pub struct NodeBridgeAuthService {
    channel: Arc<Channel>,
    check_auth: Arc<Root<JsFunction>>,
}

impl NodeBridgeAuthService {
    pub fn new(channel: Channel, check_auth: Root<JsFunction>) -> Self {
        Self {
            channel: Arc::new(channel),
            check_auth: Arc::new(check_auth),
        }
    }
}

#[async_trait]
impl SqlAuthService for NodeBridgeAuthService {
    async fn authenticate(&self, user: Option<String>) -> Result<AuthContext, CubeError> {
        trace!("[auth] Request ->");

        let request = serde_json::to_string(&user)?;
        let response: serde_json::Value = call_js_with_channel_as_callback(self.channel.clone(), self.check_auth.clone(), Some(request))
            .await?;
        trace!("[auth] Request <- {:?}", response);

        Ok(AuthContext {
            password: None,
            access_token: "fake".to_string(),
            base_path: "fake".to_string(),
        })
    }
}

di_service!(NodeBridgeAuthService, [SqlAuthService]);
