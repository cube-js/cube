use async_trait::async_trait;
use cubesql::{
    di_service,
    mysql::{AuthContext, SqlAuthService},
    CubeError,
};
use log::trace;
use neon::prelude::*;
use serde_derive::Serialize;
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

#[derive(Debug, Serialize)]
struct CheckAuthRequest {
    authorization: Option<String>,
}

#[async_trait]
impl SqlAuthService for NodeBridgeAuthService {
    async fn authenticate(&self, user: Option<String>) -> Result<AuthContext, CubeError> {
        trace!("[auth] Request ->");

        let extra = serde_json::to_string(&CheckAuthRequest {
            authorization: user.clone(),
        })?;
        let response: serde_json::Value = call_js_with_channel_as_callback(
            self.channel.clone(),
            self.check_auth.clone(),
            Some(extra),
        )
        .await?;
        trace!("[auth] Request <- {:?}", response);

        let auth_success = response.as_bool().unwrap_or(false);
        if auth_success {
            Ok(AuthContext {
                password: None,
                access_token: user.unwrap_or("fake".to_string()),
                base_path: "fake".to_string(),
            })
        } else {
            Err(CubeError::user(
                "Incorrect user name or password".to_string(),
            ))
        }
    }
}

di_service!(NodeBridgeAuthService, [SqlAuthService]);
