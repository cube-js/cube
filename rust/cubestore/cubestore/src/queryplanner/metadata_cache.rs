use bytes::Bytes;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileMeta, ParquetFileReaderFactory};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::file::encryption::ParquetEncryptionConfig;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::prelude::SessionConfig;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

/// Constructs the desired types of caches for Parquet Metadata.
pub trait MetadataCacheFactory: Sync + Send {
    /// Makes a noop cache (which doesn't cache)
    fn make_noop_cache(&self) -> Arc<dyn ParquetFileReaderFactory>;
    /// Makes an LRU-based cache.
    fn make_lru_cache(
        &self,
        max_capacity: u64,
        time_to_idle: Duration,
    ) -> Arc<dyn ParquetFileReaderFactory>;
    fn make_session_config(&self) -> SessionConfig {
        SessionConfig::new()
    }
}
/// Default MetadataCache, does not cache anything
#[derive(Debug)]
pub struct NoopParquetMetadataCache {
    default_factory: DefaultParquetFileReaderFactory,
}

impl NoopParquetMetadataCache {
    /// Creates a new DefaultMetadataCache
    pub fn new() -> Arc<Self> {
        Arc::new(NoopParquetMetadataCache {
            default_factory: DefaultParquetFileReaderFactory::new(Arc::new(
                object_store::local::LocalFileSystem::new(),
            )),
        })
    }
}

impl ParquetFileReaderFactory for NoopParquetMetadataCache {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        self.default_factory
            .create_reader(partition_index, file_meta, metadata_size_hint, metrics)
    }
}

/// LruMetadataCache, caches parquet metadata.
pub struct LruParquetMetadataCacheFactory {
    default_factory: Arc<dyn ParquetFileReaderFactory>,
    cache: Arc<moka::sync::Cache<object_store::path::Path, Arc<ParquetMetaData>>>,
}

impl LruParquetMetadataCacheFactory {
    /// Creates a new LruMetadataCache
    pub fn new(max_capacity: u64, time_to_idle: Duration) -> Arc<Self> {
        Arc::new(Self {
            default_factory: Arc::new(DefaultParquetFileReaderFactory::new(Arc::new(
                object_store::local::LocalFileSystem::new(),
            ))),
            cache: Arc::new(
                moka::sync::Cache::builder()
                    .weigher(|_, value: &Arc<ParquetMetaData>| value.memory_size() as u32)
                    .max_capacity(max_capacity)
                    .time_to_idle(time_to_idle)
                    .build(),
            ),
        })
    }
}

impl ParquetFileReaderFactory for LruParquetMetadataCacheFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        let path = file_meta.location().clone();
        let reader = self.default_factory.create_reader(
            partition_index,
            file_meta,
            metadata_size_hint,
            metrics,
        )?;

        Ok(Box::new(LruCachingFileReader {
            path,
            reader,
            cache: self.cache.clone(),
        }))
    }
}

/// Constructs regular Noop or Lru MetadataCacheFactory objects.
pub struct BasicMetadataCacheFactory {}

impl BasicMetadataCacheFactory {
    /// Constructor
    pub fn new() -> BasicMetadataCacheFactory {
        BasicMetadataCacheFactory {}
    }
}

impl MetadataCacheFactory for BasicMetadataCacheFactory {
    fn make_noop_cache(&self) -> Arc<dyn ParquetFileReaderFactory> {
        NoopParquetMetadataCache::new()
    }

    fn make_lru_cache(
        &self,
        max_capacity: u64,
        time_to_idle: Duration,
    ) -> Arc<dyn ParquetFileReaderFactory> {
        LruParquetMetadataCacheFactory::new(max_capacity, time_to_idle)
    }
}

pub struct LruCachingFileReader {
    path: object_store::path::Path,
    reader: Box<dyn AsyncFileReader>,
    cache: Arc<moka::sync::Cache<object_store::path::Path, Arc<ParquetMetaData>>>,
}

impl LruCachingFileReader {
    pub fn new(
        path: object_store::path::Path,
        reader: Box<dyn AsyncFileReader>,
        cache: Arc<moka::sync::Cache<object_store::path::Path, Arc<ParquetMetaData>>>,
    ) -> LruCachingFileReader {
        LruCachingFileReader {
            path,
            reader,
            cache,
        }
    }
}

impl AsyncFileReader for LruCachingFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        self.reader.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Vec<Bytes>>> {
        self.reader.get_byte_ranges(ranges)
    }

    fn get_metadata(
        &mut self,
        encryption_config: &Option<ParquetEncryptionConfig>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let cache = self.cache.clone();
        let path = self.path.clone();
        let encryption_config = encryption_config.clone();
        async move {
            match cache.get(&path) {
                Some(metadata) => Ok(metadata),
                None => {
                    let metadata = self.reader.get_metadata(&encryption_config).await?;
                    cache.insert(path, metadata.clone());
                    Ok(metadata)
                }
            }
        }
        .boxed()
    }
}

impl Debug for LruParquetMetadataCacheFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LruParquetMetadataCacheFactory")
            .field("cache", &"<moka::sync::Cache>")
            .field("default_factory", &self.default_factory)
            .finish()
    }
}
