use async_trait::async_trait;
use cubeclient::apis::{
    configuration::Configuration as ClientConfiguration, default_api as cube_api,
};
use cubeclient::models::{V1LoadRequest, V1LoadRequestQuery, V1LoadResponse};
use std::fmt::Debug;
use std::sync::Arc;

use crate::{compile::MetaContext, sql::AuthContext, CubeError};

#[async_trait]
pub trait TransportService: Send + Sync + Debug {
    // Load meta information about cubes
    async fn meta(&self, ctx: Arc<AuthContext>) -> Result<MetaContext, CubeError>;

    // Execute load query
    async fn load(
        &self,
        query: V1LoadRequestQuery,
        ctx: Arc<AuthContext>,
    ) -> Result<V1LoadResponse, CubeError>;
}

#[derive(Debug)]
pub struct HttpTransport;

impl HttpTransport {
    fn get_client_config_for_ctx(&self, ctx: Arc<AuthContext>) -> ClientConfiguration {
        let mut cube_config = ClientConfiguration::default();
        cube_config.bearer_access_token = Some(ctx.access_token.clone());
        cube_config.base_path = ctx.base_path.clone();

        cube_config
    }
}

crate::di_service!(HttpTransport, [TransportService]);

#[async_trait]
impl TransportService for HttpTransport {
    async fn meta(&self, ctx: Arc<AuthContext>) -> Result<MetaContext, CubeError> {
        let response = cube_api::meta_v1(&self.get_client_config_for_ctx(ctx)).await?;

        Ok(MetaContext {
            cubes: response.cubes.unwrap_or_else(Vec::new),
        })
    }

    async fn load(
        &self,
        query: V1LoadRequestQuery,
        ctx: Arc<AuthContext>,
    ) -> Result<V1LoadResponse, CubeError> {
        let request = V1LoadRequest {
            query: Some(query),
            query_type: Some("multi".to_string()),
        };
        let response =
            cube_api::load_v1(&self.get_client_config_for_ctx(ctx), Some(request)).await?;

        Ok(response)
    }
}
