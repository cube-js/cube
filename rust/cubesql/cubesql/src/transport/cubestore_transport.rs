use async_trait::async_trait;
use datafusion::arrow::{array::StringArray, datatypes::SchemaRef, record_batch::RecordBatch};
use std::{
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::compile::engine::df::scan::MemberField;
use crate::compile::engine::df::wrapper::SqlQuery;
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
use cubeclient::apis::{configuration::Configuration as CubeApiConfig, default_api as cube_api};
use std::collections::HashMap;

/// Metadata cache bucket with TTL
struct MetaCacheBucket {
    lifetime: Instant,
    value: Arc<MetaContext>,
}

/// Pre-aggregation table information from CubeStore
#[derive(Debug, Clone)]
struct PreAggTable {
    schema: String,
    table_name: String,
    cube_name: String,
    preagg_name: String,
}

impl PreAggTable {
    /// Parse table name using known cube names from Cube API metadata
    /// Format: {cube_name}_{preagg_name}_{content_hash}_{version_hash}_{timestamp}
    fn from_table_name_with_cubes(
        schema: String,
        table_name: String,
        known_cube_names: &[String],
    ) -> Option<Self> {
        // Split by underscore to find cube and preagg names
        let parts: Vec<&str> = table_name.split('_').collect();

        if parts.len() < 3 {
            return None;
        }

        // Find where hashes start (8+ char alphanumeric)
        let mut hash_start_idx = parts.len() - 3;
        for (idx, part) in parts.iter().enumerate() {
            if part.len() >= 8 && part.chars().all(|c| c.is_alphanumeric()) {
                hash_start_idx = idx;
                break;
            }
        }

        if hash_start_idx < 2 {
            return None;
        }

        // Try to match against known cube names
        // Start with longest cube names first for better matching
        let mut sorted_cubes = known_cube_names.to_vec();
        sorted_cubes.sort_by_key(|c| std::cmp::Reverse(c.len()));

        for cube_name in &sorted_cubes {
            let cube_parts: Vec<&str> = cube_name.split('_').collect();

            // Check if table name starts with this cube name
            if parts.len() >= cube_parts.len() && parts[..cube_parts.len()] == cube_parts[..] {
                // Extract pre-agg name (everything between cube name and hashes)
                let preagg_parts = &parts[cube_parts.len()..hash_start_idx];

                if preagg_parts.is_empty() {
                    continue; // Not a valid match
                }

                let preagg_name = preagg_parts.join("_");

                return Some(PreAggTable {
                    schema,
                    table_name,
                    cube_name: cube_name.clone(),
                    preagg_name,
                });
            }
        }

        // Fallback to heuristic parsing if no cube name matches
        log::warn!(
            "Could not match table '{}' to any known cube, using heuristic parsing",
            table_name
        );
        Self::from_table_name_heuristic(schema, table_name)
    }

    /// Heuristic parsing when cube names are not available
    /// Format: {cube_name}_{preagg_name}_{content_hash}_{version_hash}_{timestamp}
    fn from_table_name_heuristic(schema: String, table_name: String) -> Option<Self> {
        // Split by underscore to find cube and preagg names
        let parts: Vec<&str> = table_name.split('_').collect();

        if parts.len() < 3 {
            return None;
        }

        // Try to find the separator between cube_preagg and hashes
        // Hashes are typically 8 characters, timestamps are numeric
        // We need to work backwards to find where the preagg name ends

        // Find the first part that looks like a hash (8+ alphanumeric chars)
        let mut preagg_end_idx = parts.len() - 3; // Start from before the last 3 parts (likely hashes)

        for (idx, part) in parts.iter().enumerate() {
            if part.len() >= 8 && part.chars().all(|c| c.is_alphanumeric()) {
                preagg_end_idx = idx;
                break;
            }
        }

        if preagg_end_idx < 2 {
            return None;
        }

        // Reconstruct cube and preagg names
        let full_name = parts[..preagg_end_idx].join("_");

        // Common patterns: {cube}_{preagg}
        // Examples:
        //   mandata_captate_sums_and_count_daily -> cube=mandata_captate, preagg=sums_and_count_daily
        //   orders_with_preagg_orders_by_market_brand_daily -> cube=orders_with_preagg, preagg=orders_by_market_brand_daily

        // Strategy: Look for common pre-agg name patterns
        let (cube_name, preagg_name) = if let Some(pos) = full_name.find("_sums_") {
            // Pattern: {cube}_sums_and_count_daily
            (
                full_name[..pos].to_string(),
                full_name[pos + 1..].to_string(),
            )
        } else if let Some(pos) = full_name.find("_rollup") {
            // Pattern: {cube}_rollup_{granularity}
            (
                full_name[..pos].to_string(),
                full_name[pos + 1..].to_string(),
            )
        } else if let Some(pos) = full_name.rfind("_by_") {
            // Pattern: {cube}_{aggregation}_by_{dimensions}_{granularity}
            // Find the start of the pre-agg name by looking backwards for cube boundary
            // This is tricky - we need to find where the cube name ends

            // Heuristic: If we have "_by_", the pre-agg probably starts before it
            // Try to find common cube name endings
            let before_by = &full_name[..pos];
            if let Some(cube_end) = before_by.rfind('_') {
                (
                    before_by[..cube_end].to_string(),
                    full_name[cube_end + 1..].to_string(),
                )
            } else {
                // Can't parse, use fallback
                let mut name_parts = full_name.split('_').collect::<Vec<_>>();
                if name_parts.len() < 2 {
                    return None;
                }
                let preagg = name_parts.pop()?;
                let cube = name_parts.join("_");
                (cube, preagg.to_string())
            }
        } else {
            // Fallback: assume last 2-3 parts are preagg name
            let mut name_parts = full_name.split('_').collect::<Vec<_>>();
            if name_parts.len() < 2 {
                return None;
            }

            // Take last few parts as preagg name
            let preagg_parts = if name_parts.len() >= 4 {
                name_parts.split_off(name_parts.len() - 3)
            } else {
                vec![name_parts.pop()?]
            };

            let cube = name_parts.join("_");
            let preagg = preagg_parts.join("_");
            (cube, preagg)
        };

        Some(PreAggTable {
            schema,
            table_name,
            cube_name,
            preagg_name,
        })
    }

    fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.table_name)
    }
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

    /// Pre-aggregation table cache
    preagg_table_cache: RwLock<Option<(Instant, Vec<PreAggTable>)>>,
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
            preagg_table_cache: RwLock::new(None),
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

    /// Query CubeStore metastore to discover pre-aggregation table names
    /// Results are cached with TTL
    async fn discover_preagg_tables(&self) -> Result<Vec<PreAggTable>, CubeError> {
        let cache_lifetime = Duration::from_secs(self.config.metadata_cache_ttl);

        // Check cache first
        {
            let cache = self.preagg_table_cache.read().await;
            if let Some((timestamp, tables)) = &*cache {
                if timestamp.elapsed() < cache_lifetime {
                    log::debug!(
                        "Returning cached pre-agg tables (age: {:?}, count: {})",
                        timestamp.elapsed(),
                        tables.len()
                    );
                    return Ok(tables.clone());
                }
            }
        }

        log::debug!("Querying CubeStore metastore for pre-aggregation tables");

        // First, get cube names from Cube API metadata
        let config = self.get_cube_api_config();
        let meta_response = cube_api::meta_v1(&config, true).await.map_err(|e| {
            CubeError::internal(format!("Failed to fetch metadata from Cube API: {}", e))
        })?;

        let cubes = meta_response.cubes.unwrap_or_else(Vec::new);
        let cube_names: Vec<String> = cubes.iter().map(|cube| cube.name.clone()).collect();

        log::debug!("Known cube names from API: {:?}", cube_names);

        // Query system.tables directly from CubeStore (not through CubeSQL)
        // IMPORTANT: ORDER BY created_at DESC ensures we get the MOST RECENT version
        // of each pre-aggregation table first. Pre-agg tables can have multiple versions
        // with different hash suffixes (e.g., _abc123, _xyz789), and we want the latest.
        let sql = r#"
            SELECT
                table_schema,
                table_name
            FROM system.tables
            WHERE
                table_schema NOT IN ('information_schema', 'system', 'mysql')
                AND is_ready = true
                AND has_data = true
            ORDER BY created_at DESC
        "#;

        let batches = self.cubestore_client.query(sql.to_string()).await?;

        let mut tables = Vec::new();
        for batch in batches {
            let schema_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CubeError::internal("Invalid schema column type".to_string()))?;

            let table_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CubeError::internal("Invalid table column type".to_string()))?;

            for i in 0..batch.num_rows() {
                let schema = schema_col.value(i).to_string();
                let table_name = table_col.value(i).to_string();

                // Parse table name using known cube names
                if let Some(preagg_table) =
                    PreAggTable::from_table_name_with_cubes(schema, table_name, &cube_names)
                {
                    tables.push(preagg_table);
                } else {
                    log::warn!("Failed to parse pre-agg table name: {}", table_col.value(i));
                }
            }
        }

        log::info!(
            "Discovered {} pre-aggregation tables in CubeStore",
            tables.len()
        );
        for table in &tables {
            log::debug!(
                "  - {} (cube: {}, preagg: {})",
                table.full_name(),
                table.cube_name,
                table.preagg_name
            );
        }

        // Update cache
        {
            let mut cache = self.preagg_table_cache.write().await;
            *cache = Some((Instant::now(), tables.clone()));
        }

        Ok(tables)
    }

    /// Find the best matching pre-aggregation table for a given cube and measures/dimensions
    /// Handles both cube names (e.g., "mandata_captate") and incomplete pre-agg table names
    /// (e.g., "mandata_captate_sums_and_count_daily")
    async fn find_matching_preagg(
        &self,
        cube_name: &str,
        _measures: &[String],
        _dimensions: &[String],
    ) -> Result<Option<PreAggTable>, CubeError> {
        let tables = self.discover_preagg_tables().await?;

        // First, try to match by exact cube name
        let mut matching: Vec<PreAggTable> = tables
            .iter()
            .filter(|t| t.cube_name == cube_name)
            .cloned()
            .collect();

        // If no exact match, try to match by {cube_name}_{preagg_name} pattern
        // This handles the case where Cube.js generates SQL with incomplete pre-agg table names
        if matching.is_empty() {
            log::info!(
                "🔍 No exact cube name match for '{}', trying pre-agg pattern matching",
                cube_name
            );

            for t in &tables {
                let expected_prefix = format!("{}_{}", t.cube_name, t.preagg_name);
                log::info!(
                    "   Checking: input='{}' vs pattern='{}'",
                    cube_name,
                    expected_prefix
                );
            }

            matching = tables
                .iter()
                .filter(|t| {
                    let expected_prefix = format!("{}_{}", t.cube_name, t.preagg_name);
                    cube_name.starts_with(&expected_prefix) || cube_name == expected_prefix
                })
                .cloned()
                .collect();

            log::info!("✅ Pattern matching found {} table(s)", matching.len());
        }

        if matching.is_empty() {
            log::debug!("No pre-aggregation table found for: {}", cube_name);
            return Ok(None);
        }

        // Return the first match (most recent by naming convention)
        // TODO: Implement smarter selection based on query requirements
        let selected = matching.into_iter().next().unwrap();
        log::info!(
            "Selected pre-agg table: {} for input: {}",
            selected.full_name(),
            cube_name
        );

        Ok(Some(selected))
    }

    /// Rewrite SQL to use discovered pre-aggregation table names
    async fn rewrite_sql_for_preagg(&self, original_sql: String) -> Result<String, CubeError> {
        log::info!("🔄 Rewriting SQL for pre-aggregation routing");

        // Extract cube name from SQL
        // Simple heuristic: look for "FROM {cube_name}" pattern
        let cube_name = self.extract_cube_name_from_sql(&original_sql)?;

        log::info!(
            "📝 Extracted table name (after schema strip): '{}'",
            cube_name
        );

        // Find matching pre-aggregation table
        let preagg_table = self.find_matching_preagg(&cube_name, &[], &[]).await?;

        match preagg_table {
            Some(table) => {
                log::debug!("DEBUG: table.schema = {}", table.schema);
                log::debug!("DEBUG: table.table_name = {}", table.table_name);
                log::debug!("DEBUG: table.cube_name = {}", table.cube_name);
                log::debug!("DEBUG: table.preagg_name = {}", table.preagg_name);
                log::debug!("DEBUG: table.full_name() = {}", table.full_name());

                log::info!(
                    "Routing query to pre-aggregation table: {} (cube: {}, preagg: {})",
                    table.full_name(),
                    table.cube_name,
                    table.preagg_name
                );

                // Replace incomplete table name with full table name (with hashes)
                // Handle schema-qualified names and various patterns
                let full_name = table.full_name();

                // Patterns to replace (with and without schema prefix)
                // Try in order of specificity: most specific first
                let patterns = vec![
                    format!("{}.{}", table.schema, cube_name), // schema.incomplete_name
                    format!("\"{}\".\"{}\"", table.schema, cube_name), // "schema"."incomplete_name"
                    cube_name.to_string(),                     // incomplete_name (without schema)
                ];

                log::debug!("DEBUG: Looking for patterns to replace: {:?}", patterns);
                log::debug!("DEBUG: Will replace with: {}", full_name);

                let mut rewritten = original_sql.clone();
                let mut replaced = false;

                // Try each pattern, but stop after the first successful replacement
                for pattern in &patterns {
                    if rewritten.contains(pattern) {
                        log::debug!(
                            "DEBUG: Found pattern '{}', replacing with '{}'",
                            pattern,
                            full_name
                        );
                        rewritten = rewritten.replace(pattern, &full_name);
                        replaced = true;
                        break; // Stop after first successful replacement
                    }
                }

                if !replaced {
                    log::warn!("⚠️  No pattern matched in SQL, using original");
                }

                log::debug!("DEBUG: Rewritten SQL = {}", rewritten);

                Ok(rewritten)
            }
            None => {
                log::warn!(
                    "No pre-aggregation table found for cube '{}', using original SQL",
                    cube_name
                );
                Ok(original_sql)
            }
        }
    }

    /// Extract cube and pre-agg names from SQL query
    /// Handles both regular cube names and pre-agg table names with schema
    fn extract_cube_name_from_sql(&self, sql: &str) -> Result<String, CubeError> {
        let sql_upper = sql.to_uppercase();

        // Find "FROM" keyword
        if let Some(from_pos) = sql_upper.find("FROM") {
            let after_from = &sql[from_pos + 4..].trim_start();

            // Extract table name (until whitespace, comma, or end)
            let table_name = after_from
                .split_whitespace()
                .next()
                .ok_or_else(|| {
                    CubeError::internal("Could not extract table name from SQL".to_string())
                })?
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();

            // If table name contains schema prefix, strip it
            // Example: dev_pre_aggregations.mandata_captate_sums_and_count_daily -> mandata_captate_sums_and_count_daily
            let table_name_without_schema = if let Some(dot_pos) = table_name.rfind('.') {
                table_name[dot_pos + 1..].to_string()
            } else {
                table_name
            };

            Ok(table_name_without_schema)
        } else {
            Err(CubeError::internal(
                "Could not find FROM clause in SQL".to_string(),
            ))
        }
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

        // Get SQL query
        let original_sql = if let Some(sql_query) = sql_query {
            sql_query.sql
        } else {
            return Err(CubeError::internal(
                "Direct CubeStore queries require SQL query".to_string(),
            ));
        };

        log::info!("Original SQL: {}", original_sql);

        // Rewrite SQL to use pre-aggregation table
        let rewritten_sql = self.rewrite_sql_for_preagg(original_sql).await?;

        log::info!("Executing rewritten SQL on CubeStore: {}", rewritten_sql);

        // Execute query on CubeStore
        let batches = self.cubestore_client.query(rewritten_sql).await?;

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
                    log::debug!(
                        "Returning cached metadata (age: {:?})",
                        cache_bucket.lifetime.elapsed()
                    );
                    return Ok(cache_bucket.value.clone());
                } else {
                    log::debug!(
                        "Metadata cache expired (age: {:?})",
                        cache_bucket.lifetime.elapsed()
                    );
                }
            }
        }

        log::info!(
            "Fetching metadata from Cube API: {}",
            self.config.cube_api_url
        );

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

        // Parse pre-aggregations from cubes
        let cubes = response.cubes.unwrap_or_else(Vec::new);
        let pre_aggregations = crate::transport::service::parse_pre_aggregations_from_cubes(&cubes);

        // Create MetaContext from response
        let value = Arc::new(MetaContext::new(
            cubes,
            pre_aggregations,
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
