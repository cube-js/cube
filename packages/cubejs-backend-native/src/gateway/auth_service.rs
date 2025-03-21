use std::{any::Any, fmt::Debug, sync::Arc};

use crate::CubeError;
use async_trait::async_trait;
use serde::Serialize;

// We cannot use generic here. It's why there is this trait
// Any type will allow us to split (with downcast) auth context
pub trait GatewayAuthContext: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub type GatewayAuthContextRef = Arc<dyn GatewayAuthContext>;

#[derive(Debug)]
pub struct GatewayAuthenticateResponse {
    pub context: GatewayAuthContextRef,
}

#[derive(Debug, Serialize)]
pub struct GatewayCheckAuthRequest {
    pub(crate) protocol: String,
}

#[async_trait]
pub trait GatewayAuthService: Send + Sync + Debug {
    async fn authenticate(
        &self,
        request: GatewayCheckAuthRequest,
        token: String,
    ) -> Result<GatewayAuthenticateResponse, CubeError>;
}
