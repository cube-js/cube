use std::{any::Any, fmt::Debug, sync::Arc};

use crate::CubeError;
use async_trait::async_trait;
use serde::Serialize;

// We cannot use generic here. It's why there is this trait
// Any type will allow us to split (with downcast) auth context
pub trait GatewayAuthContext: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn user(&self) -> Option<&String>;

    fn security_context(&self) -> Option<&serde_json::Value>;
}

pub type GatewayAuthContextRef = Arc<dyn GatewayAuthContext>;

#[derive(Debug)]
pub struct GatewayAuthenticateResponse {
    pub context: GatewayAuthContextRef,
}

#[derive(Debug, Serialize)]
pub struct GatewayCheckAuthRequest {
    pub protocol: String,
}

#[derive(Debug)]
pub struct GatewayContextToApiScopesResponse {
    pub scopes: Vec<String>,
}

#[async_trait]
pub trait GatewayAuthService: Send + Sync + Debug {
    async fn authenticate(
        &self,
        req: GatewayCheckAuthRequest,
        token: String,
    ) -> Result<GatewayAuthenticateResponse, CubeError>;

    async fn context_to_api_scopes(
        &self,
        auth_context: &GatewayAuthContextRef,
    ) -> Result<GatewayContextToApiScopesResponse, CubeError>;
}
