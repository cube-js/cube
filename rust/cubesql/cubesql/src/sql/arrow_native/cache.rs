use datafusion::arrow::record_batch::RecordBatch;
use log::{debug, info};
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

/// Cache key for query results
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct QueryCacheKey {
    /// Normalized SQL query (trimmed, lowercased)
    sql: String,
    /// Optional database name
    database: Option<String>,
}

impl QueryCacheKey {
    fn new(sql: &str, database: Option<&str>) -> Self {
        Self {
            sql: normalize_query(sql),
            database: database.map(|s| s.to_string()),
        }
    }
}

/// Normalize SQL query for caching
/// Removes extra whitespace and converts to lowercase for consistent cache keys
fn normalize_query(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
        .to_lowercase()
}

/// Arrow Results Cache
///
/// This cache stores RecordBatch results from Arrow Native queries to improve
/// performance for repeated queries. The cache uses:
/// - TTL-based expiration (default 1 hour)
/// - LRU eviction policy
/// - Max size limit to prevent memory exhaustion
pub struct QueryResultCache {
    cache: Cache<QueryCacheKey, Arc<Vec<RecordBatch>>>,
    enabled: bool,
    ttl_seconds: u64,
    max_entries: u64,
}

impl QueryResultCache {
    /// Create a new Arrow Results Cache
    ///
    /// # Arguments
    /// * `enabled` - Whether caching is enabled
    /// * `max_entries` - Maximum number of cached queries (default: 1000)
    /// * `ttl_seconds` - Time to live for cached results in seconds (default: 3600 = 1 hour)
    pub fn new(enabled: bool, max_entries: u64, ttl_seconds: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_entries)
            .time_to_live(Duration::from_secs(ttl_seconds))
            .build();

        if enabled {
            info!(
                "Arrow Results Cache: ENABLED (max_entries={}, ttl={}s)",
                max_entries, ttl_seconds
            );
        } else {
            info!("Arrow Results Cache: DISABLED! Serving directly from CubeStore");
        }

        Self {
            cache,
            enabled,
            ttl_seconds,
            max_entries,
        }
    }

    /// Create cache from environment variables
    ///
    /// Environment variables:
    /// - CUBESQL_ARROW_RESULTS_CACHE_ENABLED: "true" or "false" (default: true)
    /// - CUBESQL_ARROW_RESULTS_CACHE_MAX_ENTRIES: max number of queries (default: 1000)
    /// - CUBESQL_ARROW_RESULTS_CACHE_TTL: TTL in seconds (default: 3600)
    pub fn from_env() -> Self {
        let enabled = std::env::var("CUBESQL_ARROW_RESULTS_CACHE_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);

        let max_entries = std::env::var("CUBESQL_ARROW_RESULTS_CACHE_MAX_ENTRIES")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .unwrap_or(1000);

        let ttl_seconds = std::env::var("CUBESQL_ARROW_RESULTS_CACHE_TTL")
            .unwrap_or_else(|_| "3600".to_string())
            .parse()
            .unwrap_or(3600);

        Self::new(enabled, max_entries, ttl_seconds)
    }

    /// Try to get cached result for a query
    ///
    /// Returns None if:
    /// - Cache is disabled
    /// - Query is not in cache
    /// - Cache entry has expired
    pub async fn get(&self, sql: &str, database: Option<&str>) -> Option<Arc<Vec<RecordBatch>>> {
        if !self.enabled {
            return None;
        }

        let key = QueryCacheKey::new(sql, database);
        let result = self.cache.get(&key).await;

        if result.is_some() {
            debug!(
                "Cache HIT for query: {}",
                &key.sql[..std::cmp::min(key.sql.len(), 100)]
            );
        } else {
            debug!(
                "Cache MISS for query: {}",
                &key.sql[..std::cmp::min(key.sql.len(), 100)]
            );
        }

        result
    }

    /// Insert query result into cache
    ///
    /// Only caches if:
    /// - Cache is enabled
    /// - Batches are not empty
    pub async fn insert(&self, sql: &str, database: Option<&str>, batches: Vec<RecordBatch>) {
        if !self.enabled {
            return;
        }

        if batches.is_empty() {
            debug!("Skipping cache insert for empty result set");
            return;
        }

        let key = QueryCacheKey::new(sql, database);
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        let batch_count = batches.len();

        debug!(
            "Caching query result: {} rows in {} batches, query: {}",
            row_count,
            batch_count,
            &key.sql[..std::cmp::min(key.sql.len(), 100)]
        );

        self.cache.insert(key, Arc::new(batches)).await;
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            enabled: self.enabled,
            entry_count: self.cache.entry_count(),
            max_entries: self.max_entries,
            ttl_seconds: self.ttl_seconds,
            weighted_size: self.cache.weighted_size(),
        }
    }

    /// Clear all cached entries
    pub async fn clear(&self) {
        if self.enabled {
            info!("Clearing Arrow Results Cache");
            self.cache.invalidate_all();
            // Optionally wait for invalidation to complete
            self.cache.run_pending_tasks().await;
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub enabled: bool,
    pub entry_count: u64,
    pub max_entries: u64,
    pub ttl_seconds: u64,
    pub weighted_size: u64,
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryCache[enabled={}, entries={}/{}, ttl={}s, size={}]",
            self.enabled, self.entry_count, self.max_entries, self.ttl_seconds, self.weighted_size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(size: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1; size]);
        let name_array = StringArray::from(vec!["test"; size]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[tokio::test]
    async fn test_cache_basic() {
        let cache = QueryResultCache::new(true, 10, 3600);
        let batch = create_test_batch(10);

        // Cache miss
        assert!(cache.get("SELECT * FROM test", None).await.is_none());

        // Insert
        cache
            .insert("SELECT * FROM test", None, vec![batch.clone()])
            .await;

        // Cache hit
        let cached = cache.get("SELECT * FROM test", None).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_cache_normalization() {
        let cache = QueryResultCache::new(true, 10, 3600);
        let batch = create_test_batch(10);

        // Insert with extra whitespace
        cache
            .insert("  SELECT   *   FROM   test  ", None, vec![batch.clone()])
            .await;

        // Should hit cache with different whitespace
        assert!(cache.get("SELECT * FROM test", None).await.is_some());
        assert!(cache.get("select * from test", None).await.is_some());
    }

    #[tokio::test]
    async fn test_cache_disabled() {
        let cache = QueryResultCache::new(false, 10, 3600);
        let batch = create_test_batch(10);

        // Insert when disabled
        cache.insert("SELECT * FROM test", None, vec![batch]).await;

        // Should not cache
        assert!(cache.get("SELECT * FROM test", None).await.is_none());
    }

    #[tokio::test]
    async fn test_cache_database_scope() {
        let cache = QueryResultCache::new(true, 10, 3600);
        let batch1 = create_test_batch(10);
        let batch2 = create_test_batch(20);

        // Insert same query for different databases
        cache.insert("SELECT * FROM test", None, vec![batch1]).await;
        cache
            .insert("SELECT * FROM test", Some("db1"), vec![batch2])
            .await;

        // Should have separate cache entries
        let result1 = cache.get("SELECT * FROM test", None).await;
        let result2 = cache.get("SELECT * FROM test", Some("db1")).await;

        assert!(result1.is_some());
        assert!(result2.is_some());
        assert_eq!(result1.unwrap()[0].num_rows(), 10);
        assert_eq!(result2.unwrap()[0].num_rows(), 20);
    }

    #[tokio::test]
    async fn test_empty_results_not_cached() {
        let cache = QueryResultCache::new(true, 10, 3600);

        cache.insert("SELECT * FROM empty", None, vec![]).await;

        // Empty results should not be cached
        assert!(cache.get("SELECT * FROM empty", None).await.is_none());
    }
}
