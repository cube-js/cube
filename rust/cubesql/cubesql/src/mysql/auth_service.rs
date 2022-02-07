use std::{env, fmt::Debug, sync::Arc};

use async_trait::async_trait;

use crate::CubeError;

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub access_token: String,
    pub base_path: String,
}

#[derive(Debug)]
pub struct AuthenticateResponse {
    pub(crate) context: AuthContext,
    pub(crate) password: Option<String>,
}

impl AuthenticateResponse {
    pub fn new(context: AuthContext, password: Option<String>) -> Self {
        Self { context, password }
    }
}

#[async_trait]
pub trait SqlAuthService: Send + Sync + Debug {
    async fn authenticate(&self, user: Option<String>) -> Result<AuthenticateResponse, CubeError>;
}

#[derive(Debug)]
pub struct SqlAuthDefaultImpl;

crate::di_service!(SqlAuthDefaultImpl, [SqlAuthService]);

#[async_trait]
impl SqlAuthService for SqlAuthDefaultImpl {
    async fn authenticate(&self, _user: Option<String>) -> Result<AuthenticateResponse, CubeError> {
        Ok(AuthenticateResponse {
            context: AuthContext {
                access_token: env::var("CUBESQL_CUBE_TOKEN")
                    .ok()
                    .unwrap_or_else(|| panic!("CUBESQL_CUBE_TOKEN is a required ENV variable")),
                base_path: env::var("CUBESQL_CUBE_URL")
                    .ok()
                    .unwrap_or_else(|| panic!("CUBESQL_CUBE_URL is a required ENV variable")),
            },
            password: None,
        })
    }
}
