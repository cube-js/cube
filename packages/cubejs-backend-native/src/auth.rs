use async_trait::async_trait;
use cubesql::{
    di_service,
    sql::{AuthContext, AuthenticateResponse, SqlAuthService},
    CubeError,
};
use log::trace;
use neon::prelude::*;
use serde::Deserialize;
use serde_derive::Serialize;
use std::sync::Arc;
use uuid::Uuid;

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
pub struct TransportRequest {
    pub id: String,
}

#[derive(Debug, Serialize)]
struct CheckAuthRequest {
    request: TransportRequest,
    user: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CheckAuthResponse {
    password: Option<String>,
}

#[async_trait]
impl SqlAuthService for NodeBridgeAuthService {
    async fn authenticate(&self, user: Option<String>) -> Result<AuthenticateResponse, CubeError> {
        trace!("[auth] Request ->");

        let request_id = Uuid::new_v4().to_string();

        let extra = serde_json::to_string(&CheckAuthRequest {
            request: TransportRequest {
                id: format!("{}-span-1", request_id),
            },
            user: user.clone(),
        })?;
        let response: CheckAuthResponse = call_js_with_channel_as_callback(
            self.channel.clone(),
            self.check_auth.clone(),
            Some(extra),
        )
        .await?;
        trace!("[auth] Request <- {:?}", response);

        Ok(AuthenticateResponse::new(
            AuthContext {
                access_token: user.unwrap_or_else(|| "fake".to_string()),
                base_path: "fake".to_string(),
            },
            response.password,
        ))
    }
}

di_service!(NodeBridgeAuthService, [SqlAuthService]);
