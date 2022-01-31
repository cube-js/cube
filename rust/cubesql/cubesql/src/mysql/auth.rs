use async_trait::async_trait;
use std::sync::Arc;

use crate::CubeError;

#[derive(Debug)]
pub struct AuthContext {
    pub password: Option<String>,
}

#[async_trait]
pub trait SqlAuthService: Send + Sync {
    async fn authenticate(&self, user: Option<String>) -> Result<AuthContext, CubeError>;
}

pub struct SqlAuthDefaultImpl;

crate::di_service!(SqlAuthDefaultImpl, [SqlAuthService]);

#[async_trait]
impl SqlAuthService for SqlAuthDefaultImpl {
    async fn authenticate(&self, _user: Option<String>) -> Result<AuthContext, CubeError> {
        Ok(AuthContext { password: None })
    }
}
