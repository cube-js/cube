use async_trait::async_trait;
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::{fmt::Debug, sync::Arc, time::{Duration, Instant}};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    compile::engine::df::scan::CacheMode,
    cubestore::client::CubeStoreClient,
    sql::AuthContextRef,
    transport::{
        CubeStreamReceiver, LoadRequestMeta, MetaContext, SpanId, SqlResponse,
        TransportLoadRequestQuery, TransportService,
    },
    CubeError,
};
use crate::compile::engine::df::scan::MemberField;
use crate::compile::engine::df::wrapper::SqlQuery;
use cubeclient::apis::{configuration::Configuration as CubeApiConfig, default_api as cube_api};
use std::collections::HashMap;

/// Metadata cache bucket with TTL
struct MetaCacheBucket {
    lifetime: Instant,
    value: Arc<MetaContext>,
}

/// Configuration for CubeStore direct connection
#[derive(Debug, Clone)]
pub struct CubeStoreTransportConfig {
    /// Enable direct CubeStore queries
    pub enabled: bool,

    /// Cube API URL for metadata fetching
    pub cube_api_url: String,

    /// CubeStore WebSocket URL
    pub cubestore_url: String,

    /// Metadata cache TTL (seconds)
    pub metadata_cache_ttl: u64,
}

impl Default for CubeStoreTransportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cube_api_url: "http://localhost:4000/cubejs-api".to_string(),
            cubestore_url: "ws://127.0.0.1:3030/ws".to_string(),
            metadata_cache_ttl: 300,
        }
    }
}

impl CubeStoreTransportConfig {
    pub fn from_env() -> Result<Self, CubeError> {
        Ok(Self {
            enabled: std::env::var("CUBESQL_CUBESTORE_DIRECT")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            cube_api_url: std::env::var("CUBESQL_CUBE_URL")
                .unwrap_or_else(|_| "http://localhost:4000/cubejs-api".to_string()),
            cubestore_url: std::env::var("CUBESQL_CUBESTORE_URL")
                .unwrap_or_else(|_| "ws://127.0.0.1:3030/ws".to_string()),
            metadata_cache_ttl: std::env::var("CUBESQL_METADATA_CACHE_TTL")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
        })
    }
}

/// Transport implementation that connects directly to CubeStore
/// This bypasses the Cube API HTTP/JSON layer for data transfer
pub struct CubeStoreTransport {
    /// Direct WebSocket client to CubeStore
    cubestore_client: Arc<CubeStoreClient>,

    /// Configuration
    config: CubeStoreTransportConfig,

    /// Metadata cache with TTL
    meta_cache: RwLock<Option<MetaCacheBucket>>,
}

impl std::fmt::Debug for CubeStoreTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CubeStoreTransport")
            .field("cubestore_client", &self.cubestore_client)
            .field("config", &self.config)
            .field("meta_cache", &"<RwLock>")
            .finish()
    }
}

impl CubeStoreTransport {
    pub fn new(config: CubeStoreTransportConfig) -> Result<Self, CubeError> {
        log::info!(
            "Initializing CubeStoreTransport (enabled: {}, cube_api: {}, cubestore: {})",
            config.enabled,
            config.cube_api_url,
            config.cubestore_url
        );

        let cubestore_client = Arc::new(CubeStoreClient::new(config.cubestore_url.clone()));

        Ok(Self {
            cubestore_client,
            config,
            meta_cache: RwLock::new(None),
        })
    }

    /// Get Cube API client configuration
    fn get_cube_api_config(&self) -> CubeApiConfig {
        let mut config = CubeApiConfig::default();
        config.base_path = self.config.cube_api_url.clone();
        config
    }

    /// Check if we should use direct CubeStore connection for this query
    fn should_use_direct(&self) -> bool {
        self.config.enabled
    }

    /// Execute query directly against CubeStore
    async fn load_direct(
        &self,
        _span_id: Option<Arc<SpanId>>,
        query: TransportLoadRequestQuery,
        sql_query: Option<SqlQuery>,
        _ctx: AuthContextRef,
        _meta_fields: LoadRequestMeta,
        _schema: SchemaRef,
        _member_fields: Vec<MemberField>,
        _cache_mode: Option<CacheMode>,
    ) -> Result<Vec<RecordBatch>, CubeError> {
        log::debug!("Executing query directly against CubeStore: {:?}", query);

        // For now, use the SQL query if provided
        // TODO: Use cubesqlplanner to generate optimized SQL with pre-aggregation selection
        let sql = if let Some(sql_query) = sql_query {
            sql_query.sql
        } else {
            // Fallback: construct a simple SQL from query parts
            // This is a placeholder - in production we'll use cubesqlplanner
            return Err(CubeError::internal(
                "Direct CubeStore queries require SQL query".to_string(),
            ));
        };

        log::info!("Executing SQL on CubeStore: {}", sql);

        // Execute query on CubeStore
        let batches = self.cubestore_client.query(sql).await?;

        log::debug!("Query returned {} batches", batches.len());

        Ok(batches)
    }
}

#[async_trait]
impl TransportService for CubeStoreTransport {
    async fn meta(&self, _ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
        let cache_lifetime = Duration::from_secs(self.config.metadata_cache_ttl);

        // Check cache first (read lock)
        {
            let store = self.meta_cache.read().await;
            if let Some(cache_bucket) = &*store {
                if cache_bucket.lifetime.elapsed() < cache_lifetime {
                    log::debug!("Returning cached metadata (age: {:?})", cache_bucket.lifetime.elapsed());
                    return Ok(cache_bucket.value.clone());
                } else {
                    log::debug!("Metadata cache expired (age: {:?})", cache_bucket.lifetime.elapsed());
                }
            }
        }

        log::info!("Fetching metadata from Cube API: {}", self.config.cube_api_url);

        // Fetch metadata from Cube API
        let config = self.get_cube_api_config();
        let response = cube_api::meta_v1(&config, true).await.map_err(|e| {
            CubeError::internal(format!("Failed to fetch metadata from Cube API: {}", e))
        })?;

        log::info!("Successfully fetched metadata from Cube API");

        // Acquire write lock
        let mut store = self.meta_cache.write().await;

        // Double-check cache (another thread might have updated it)
        if let Some(cache_bucket) = &*store {
            if cache_bucket.lifetime.elapsed() < cache_lifetime {
                log::debug!("Cache was updated by another thread, using that");
                return Ok(cache_bucket.value.clone());
            }
        }

        // Create MetaContext from response
        let value = Arc::new(MetaContext::new(
            response.cubes.unwrap_or_else(Vec::new),
            HashMap::new(), // member_to_data_source not used in standalone mode
            HashMap::new(), // data_source_to_sql_generator not used in standalone mode
            Uuid::new_v4(),
        ));

        log::debug!("Cached metadata with {} cubes", value.cubes.len());

        // Store in cache
        *store = Some(MetaCacheBucket {
            lifetime: Instant::now(),
            value: value.clone(),
        });

        Ok(value)
    }

    async fn sql(
        &self,
        _span_id: Option<Arc<SpanId>>,
        _query: TransportLoadRequestQuery,
        _ctx: AuthContextRef,
        _meta_fields: LoadRequestMeta,
        _member_to_alias: Option<HashMap<String, String>>,
        _expression_params: Option<Vec<Option<String>>>,
    ) -> Result<SqlResponse, CubeError> {
        // TODO: Use cubesqlplanner to generate SQL
        Err(CubeError::internal(
            "CubeStoreTransport.sql() not implemented yet - use fallback transport".to_string(),
        ))
    }

    async fn load(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: TransportLoadRequestQuery,
        sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
        cache_mode: Option<CacheMode>,
    ) -> Result<Vec<RecordBatch>, CubeError> {
        if !self.should_use_direct() {
            return Err(CubeError::internal(
                "CubeStore direct mode not enabled".to_string(),
            ));
        }

        match self
            .load_direct(
                span_id,
                query,
                sql_query,
                ctx,
                meta_fields,
                schema,
                member_fields,
                cache_mode,
            )
            .await
        {
            Ok(batches) => {
                log::info!("Query executed successfully via direct CubeStore connection");
                Ok(batches)
            }
            Err(err) => {
                log::warn!(
                    "CubeStore direct query failed: {} - need fallback transport",
                    err
                );
                Err(err)
            }
        }
    }

    async fn load_stream(
        &self,
        _span_id: Option<Arc<SpanId>>,
        _query: TransportLoadRequestQuery,
        _sql_query: Option<SqlQuery>,
        _ctx: AuthContextRef,
        _meta_fields: LoadRequestMeta,
        _schema: SchemaRef,
        _member_fields: Vec<MemberField>,
    ) -> Result<CubeStreamReceiver, CubeError> {
        // TODO: Implement streaming support
        Err(CubeError::internal(
            "Streaming not yet supported for CubeStore direct".to_string(),
        ))
    }

    async fn log_load_state(
        &self,
        _span_id: Option<Arc<SpanId>>,
        _ctx: AuthContextRef,
        _meta_fields: LoadRequestMeta,
        _event: String,
        _properties: serde_json::Value,
    ) -> Result<(), CubeError> {
        // Logging is optional, just return Ok
        Ok(())
    }

    async fn can_switch_user_for_session(
        &self,
        _ctx: AuthContextRef,
        _to_user: String,
    ) -> Result<bool, CubeError> {
        // Delegate user switching to Cube API
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = CubeStoreTransportConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.cube_api_url, "http://localhost:4000/cubejs-api");
        assert_eq!(config.cubestore_url, "ws://127.0.0.1:3030/ws");
        assert_eq!(config.metadata_cache_ttl, 300);
    }

    #[test]
    fn test_config_from_env() {
        std::env::set_var("CUBESQL_CUBESTORE_DIRECT", "true");
        std::env::set_var("CUBESQL_CUBE_URL", "http://localhost:4008/cubejs-api");
        std::env::set_var("CUBESQL_CUBESTORE_URL", "ws://localhost:3030/ws");
        std::env::set_var("CUBESQL_METADATA_CACHE_TTL", "600");

        let config = CubeStoreTransportConfig::from_env().unwrap();
        assert!(config.enabled);
        assert_eq!(config.cube_api_url, "http://localhost:4008/cubejs-api");
        assert_eq!(config.cubestore_url, "ws://localhost:3030/ws");
        assert_eq!(config.metadata_cache_ttl, 600);

        std::env::remove_var("CUBESQL_CUBESTORE_DIRECT");
        std::env::remove_var("CUBESQL_CUBE_URL");
        std::env::remove_var("CUBESQL_CUBESTORE_URL");
        std::env::remove_var("CUBESQL_METADATA_CACHE_TTL");
    }

    #[test]
    fn test_transport_creation() {
        let config = CubeStoreTransportConfig::default();
        let transport = CubeStoreTransport::new(config);
        assert!(transport.is_ok());
    }
}

// Register CubeStoreTransport for dependency injection
crate::di_service!(CubeStoreTransport, [TransportService]);
