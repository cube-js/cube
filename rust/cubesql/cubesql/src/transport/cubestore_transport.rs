use async_trait::async_trait;
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::{fmt::Debug, sync::Arc};

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
use std::collections::HashMap;

/// Configuration for CubeStore direct connection
#[derive(Debug, Clone)]
pub struct CubeStoreTransportConfig {
    /// Enable direct CubeStore queries
    pub enabled: bool,

    /// CubeStore WebSocket URL
    pub cubestore_url: String,

    /// Metadata cache TTL (seconds)
    pub metadata_cache_ttl: u64,
}

impl Default for CubeStoreTransportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
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
#[derive(Debug)]
pub struct CubeStoreTransport {
    /// Direct WebSocket client to CubeStore
    cubestore_client: Arc<CubeStoreClient>,

    /// HTTP transport for Cube API (metadata fallback)
    /// TODO: Add HTTP transport for metadata fetching
    /// cube_api_client: Arc<dyn TransportService>,

    /// Configuration
    config: CubeStoreTransportConfig,
}

impl CubeStoreTransport {
    pub fn new(config: CubeStoreTransportConfig) -> Result<Self, CubeError> {
        log::info!(
            "Initializing CubeStoreTransport (enabled: {}, url: {})",
            config.enabled,
            config.cubestore_url
        );

        let cubestore_client = Arc::new(CubeStoreClient::new(config.cubestore_url.clone()));

        Ok(Self {
            cubestore_client,
            config,
        })
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
        // TODO: Fetch metadata from Cube API
        // For now, return error to use fallback transport
        Err(CubeError::internal(
            "CubeStoreTransport.meta() not implemented yet - use fallback transport".to_string(),
        ))
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
        assert_eq!(config.cubestore_url, "ws://127.0.0.1:3030/ws");
        assert_eq!(config.metadata_cache_ttl, 300);
    }

    #[test]
    fn test_config_from_env() {
        std::env::set_var("CUBESQL_CUBESTORE_DIRECT", "true");
        std::env::set_var("CUBESQL_CUBESTORE_URL", "ws://localhost:3030/ws");
        std::env::set_var("CUBESQL_METADATA_CACHE_TTL", "600");

        let config = CubeStoreTransportConfig::from_env().unwrap();
        assert!(config.enabled);
        assert_eq!(config.cubestore_url, "ws://localhost:3030/ws");
        assert_eq!(config.metadata_cache_ttl, 600);

        std::env::remove_var("CUBESQL_CUBESTORE_DIRECT");
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
