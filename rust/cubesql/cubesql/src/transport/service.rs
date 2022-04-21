use async_trait::async_trait;
use cubeclient::apis::{
    configuration::Configuration as ClientConfiguration, default_api as cube_api,
};
use cubeclient::models::{V1LoadRequest, V1LoadRequestQuery, V1LoadResponse};

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock as RwLockAsync;
use tokio::time::Instant;

use crate::{compile::MetaContext, sql::AuthContext, CubeError};

#[async_trait]
pub trait TransportService: Send + Sync + Debug {
    // Load meta information about cubes
    async fn meta(&self, ctx: Arc<AuthContext>) -> Result<Arc<MetaContext>, CubeError>;

    // Execute load query
    async fn load(
        &self,
        query: V1LoadRequestQuery,
        ctx: Arc<AuthContext>,
    ) -> Result<V1LoadResponse, CubeError>;
}

#[derive(Debug)]
struct MetaCacheBucket {
    lifetime: Instant,
    value: Arc<MetaContext>,
}

/// This transports is used in standalone mode
#[derive(Debug)]
pub struct HttpTransport {
    /// We use simple cache to improve DX with standalone mode
    /// because currently we dont persist DF in the SessionState
    /// and it causes a lot of HTTP requests which slow down BI connections
    cache: RwLockAsync<Option<MetaCacheBucket>>,
}

const CACHE_LIFETIME_DURATION: Duration = Duration::from_secs(5);

impl HttpTransport {
    pub fn new() -> Self {
        Self {
            cache: RwLockAsync::new(None),
        }
    }

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
    async fn meta(&self, ctx: Arc<AuthContext>) -> Result<Arc<MetaContext>, CubeError> {
        {
            let store = self.cache.read().await;
            if let Some(cache_bucket) = &*store {
                if cache_bucket.lifetime.elapsed() < CACHE_LIFETIME_DURATION {
                    return Ok(cache_bucket.value.clone());
                };
            };
        }

        let response = cube_api::meta_v1(&self.get_client_config_for_ctx(ctx)).await?;

        let mut store = self.cache.write().await;
        if let Some(cache_bucket) = &*store {
            if cache_bucket.lifetime.elapsed() < CACHE_LIFETIME_DURATION {
                return Ok(cache_bucket.value.clone());
            }
        };

        let value = Arc::new(MetaContext::new(response.cubes.unwrap_or_else(Vec::new)));

        *store = Some(MetaCacheBucket {
            lifetime: Instant::now(),
            value: value.clone(),
        });

        Ok(value)
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
