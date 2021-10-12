use async_trait::async_trait;
use cubesql::{
    di_service,
    mysql::{AuthContext, SqlAuthService},
    CubeError,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct NodeBridgeAuthService {
    channel: Arc<Channel>,
    on_load: Arc<Root<JsFunction>>,
}

impl NodeBridgeAuthService {
    pub fn new(channel: Channel, on_auth: Root<JsFunction>) -> NodeBridgeAuthService {
        NodeBridgeAuthService {
            channel: Arc::new(channel),
            on_auth: Arc::new(on_auth),
        }
    }
}

#[async_trait]
impl SqlAuthService for NodeBridgeAuthService {
    async fn authenticate(&self, _: Option<String>) -> Result<AuthContext, CubeError> {
        Ok(AuthContext {
            password: None,
            access_token: "fake".to_string(),
            base_path: "fake".to_string(),
        })
    }
}

di_service!(NodeBridgeAuthService, [SqlAuthService]);
