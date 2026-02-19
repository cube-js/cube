use crate::{
    compile::engine::df::{
        scan::{CacheMode, MemberField},
        wrapper::SqlQuery,
    },
    sql::AuthContextRef,
    transport::{
        CubeStoreTransport, CubeStoreTransportConfig, HttpTransport, LoadRequestMeta,
        TransportLoadRequestQuery, TransportService,
    },
    CubeError,
};
use async_trait::async_trait;
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::{collections::HashMap, sync::Arc};

use super::{
    ctx::MetaContext,
    service::{CubeStreamReceiver, SpanId, SqlResponse},
};

/// Hybrid transport that combines HttpTransport and CubeStoreTransport
///
/// This transport intelligently routes queries:
/// - Queries WITH SQL → CubeStoreTransport (direct CubeStore, fast)
/// - Queries WITHOUT SQL → HttpTransport (Cube API, handles MEASURE syntax)
#[derive(Debug)]
pub struct HybridTransport {
    http_transport: Arc<HttpTransport>,
    cubestore_transport: Option<Arc<CubeStoreTransport>>,
}

impl HybridTransport {
    pub fn new() -> Result<Self, CubeError> {
        let http_transport = Arc::new(HttpTransport::new());

        // Try to initialize CubeStoreTransport if configured
        let cubestore_transport = match CubeStoreTransportConfig::from_env() {
            Ok(config) if config.enabled => match CubeStoreTransport::new(config) {
                Ok(transport) => {
                    log::info!("✅ HybridTransport initialized with CubeStore direct support");
                    Some(Arc::new(transport))
                }
                Err(e) => {
                    log::warn!(
                        "⚠️  Failed to initialize CubeStore direct mode: {}. Using HTTP-only.",
                        e
                    );
                    None
                }
            },
            _ => {
                log::info!("HybridTransport initialized (HTTP-only, CubeStore direct disabled)");
                None
            }
        };

        Ok(Self {
            http_transport,
            cubestore_transport,
        })
    }
}

#[async_trait]
impl TransportService for HybridTransport {
    async fn meta(&self, ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
        // Use CubeStoreTransport if available (it caches metadata from Cube API)
        // Otherwise use HttpTransport
        if let Some(ref cubestore) = self.cubestore_transport {
            cubestore.meta(ctx).await
        } else {
            self.http_transport.meta(ctx).await
        }
    }

    async fn sql(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: TransportLoadRequestQuery,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        member_to_alias: Option<HashMap<String, String>>,
        expression_params: Option<Vec<Option<String>>>,
    ) -> Result<SqlResponse, CubeError> {
        // SQL endpoint always goes through HTTP transport
        // This is used for query compilation, not execution
        self.http_transport
            .sql(
                span_id,
                query,
                ctx,
                meta_fields,
                member_to_alias,
                expression_params,
            )
            .await
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
        // Route based on whether we have an SQL query
        if let Some(ref sql_query) = sql_query {
            if let Some(ref cubestore) = self.cubestore_transport {
                log::info!(
                    "🚀 Routing to CubeStore direct (SQL length: {} chars)",
                    sql_query.sql.len()
                );

                // Try CubeStore first
                match cubestore
                    .load(
                        span_id.clone(),
                        query.clone(),
                        Some(sql_query.clone()),
                        ctx.clone(),
                        meta_fields.clone(),
                        schema.clone(),
                        member_fields.clone(),
                        cache_mode.clone(),
                    )
                    .await
                {
                    Ok(result) => {
                        log::info!("✅ CubeStore direct query succeeded");
                        return Ok(result);
                    }
                    Err(e) => {
                        log::warn!("⚠️  CubeStore direct query failed: {}. Falling back to HTTP transport.", e);
                        // Fall through to HTTP transport
                    }
                }
            }
        } else {
            log::info!("Routing to HTTP transport (no SQL query, likely MEASURE syntax)");
        }

        // Fallback to HTTP transport
        self.http_transport
            .load(
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
    }

    async fn load_stream(
        &self,
        span_id: Option<Arc<SpanId>>,
        query: TransportLoadRequestQuery,
        sql_query: Option<SqlQuery>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        schema: SchemaRef,
        member_fields: Vec<MemberField>,
    ) -> Result<CubeStreamReceiver, CubeError> {
        // For now, always use HTTP transport for streaming
        // TODO: Implement streaming for CubeStore direct
        self.http_transport
            .load_stream(
                span_id,
                query,
                sql_query,
                ctx,
                meta_fields,
                schema,
                member_fields,
            )
            .await
    }

    async fn can_switch_user_for_session(
        &self,
        ctx: AuthContextRef,
        to_user: String,
    ) -> Result<bool, CubeError> {
        // Use HTTP transport for session management
        self.http_transport
            .can_switch_user_for_session(ctx, to_user)
            .await
    }

    async fn log_load_state(
        &self,
        span_id: Option<Arc<SpanId>>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
        event: String,
        properties: serde_json::Value,
    ) -> Result<(), CubeError> {
        // Use HTTP transport for logging
        self.http_transport
            .log_load_state(span_id, ctx, meta_fields, event, properties)
            .await
    }
}

// Register HybridTransport for dependency injection
crate::di_service!(HybridTransport, [TransportService]);
