use std::{any::Any, env, fmt::Debug, sync::Arc};

use async_trait::async_trait;

use crate::CubeError;

// We cannot use generic here. It's why there is this trait
// Any type will allow us to split (with downcast) auth context into HTTP (standalone) or Native
pub trait AuthContext: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub type AuthContextRef = Arc<dyn AuthContext>;

#[derive(Debug, Clone)]
pub struct HttpAuthContext {
    pub access_token: String,
    pub base_path: String,
}

impl AuthContext for HttpAuthContext {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct AuthenticateResponse {
    pub context: AuthContextRef,
    pub password: Option<String>,
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
            context: Arc::new(HttpAuthContext {
                access_token: env::var("CUBESQL_CUBE_TOKEN")
                    .ok()
                    .unwrap_or_else(|| panic!("CUBESQL_CUBE_TOKEN is a required ENV variable")),
                base_path: env::var("CUBESQL_CUBE_URL")
                    .ok()
                    .unwrap_or_else(|| panic!("CUBESQL_CUBE_URL is a required ENV variable")),
            }),
            password: None,
        })
    }
}
