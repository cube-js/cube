use crate::cachestore::cache_eviction_manager::EvictionResult;
use crate::cachestore::cache_rocksstore::{
    CachestoreInfo, QueueAddPayload, QueueAddResponse, QueueAllItem, QueueGetResponse,
    QueueListItem,
};
use crate::cachestore::queue_item::QueueRetrieveResponse;
use crate::cachestore::{
    CacheItem, CacheStore, QueueCancelResponse, QueueItem, QueueItemStatus, QueueKey, QueueResult,
    QueueResultResponse, RocksCacheStore,
};
use crate::config::ConfigObj;
use crate::metastore::{IdRow, MetaStoreEvent, MetaStoreFs, RocksPropertyRow};
use crate::CubeError;
use async_trait::async_trait;
use log::trace;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch::{Receiver, Sender};
use tokio::task::JoinHandle;

pub enum LazyRocksCacheStoreState {
    FromRemote {
        path: String,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        listeners: Vec<tokio::sync::broadcast::Sender<MetaStoreEvent>>,
        _init_flag: Sender<bool>,
    },
    Closed {},
    Initialized {
        store: Arc<RocksCacheStore>,
    },
}

pub struct LazyRocksCacheStore {
    init_signal: Option<Receiver<bool>>,
    state: tokio::sync::RwLock<LazyRocksCacheStoreState>,
}

impl LazyRocksCacheStore {
    pub async fn load_from_dump(
        path: &Path,
        dump_path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        listeners: Vec<tokio::sync::broadcast::Sender<MetaStoreEvent>>,
    ) -> Result<Arc<Self>, CubeError> {
        let store = RocksCacheStore::load_from_dump(path, dump_path, metastore_fs, config).await?;

        for listener in listeners {
            store.add_listener(listener).await;
        }

        Ok(Arc::new(Self {
            init_signal: None,
            state: tokio::sync::RwLock::new(LazyRocksCacheStoreState::Initialized { store }),
        }))
    }

    pub async fn load_from_remote(
        path: &str,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        listeners: Vec<tokio::sync::broadcast::Sender<MetaStoreEvent>>,
    ) -> Result<Arc<Self>, CubeError> {
        let (init_flag, init_signal) = tokio::sync::watch::channel::<bool>(false);

        Ok(Arc::new(Self {
            init_signal: Some(init_signal),
            state: tokio::sync::RwLock::new(LazyRocksCacheStoreState::FromRemote {
                path: path.to_string(),
                metastore_fs,
                config,
                listeners,
                _init_flag: init_flag,
            }),
        }))
    }

    async fn init(&self) -> Result<Arc<RocksCacheStore>, CubeError> {
        {
            let guard = self.state.read().await;
            match &*guard {
                LazyRocksCacheStoreState::FromRemote { .. } => {}
                LazyRocksCacheStoreState::Closed { .. } => {
                    return Err(CubeError::internal(
                        "Unable to initialize Cache Store on lazy call, it was closed".to_string(),
                    ));
                }
                LazyRocksCacheStoreState::Initialized { store } => {
                    return Ok(store.clone());
                }
            }
        }

        let mut guard = self.state.write().await;
        match &*guard {
            LazyRocksCacheStoreState::FromRemote {
                path,
                metastore_fs,
                config,
                listeners,
                // receiver will be closed on drop
                _init_flag: _,
            } => {
                let store =
                    RocksCacheStore::load_from_remote(&path, metastore_fs.clone(), config.clone())
                        .await?;

                for listener in listeners {
                    store.add_listener(listener.clone()).await;
                }

                *guard = LazyRocksCacheStoreState::Initialized {
                    store: store.clone(),
                };

                Ok(store)
            }
            _ => Err(CubeError::internal(
                "Unable to initialize Cache Store on lazy call, unexpected state".to_string(),
            )),
        }
    }

    pub async fn spawn_processing_loops(self: Arc<Self>) -> Vec<JoinHandle<Result<(), CubeError>>> {
        if let Some(init_signal) = &self.init_signal {
            let _ = init_signal.clone().changed().await;
        }

        let store = {
            let guard = self.state.read().await;
            if let LazyRocksCacheStoreState::Initialized { store } = &*guard {
                store.clone()
            } else {
                return vec![];
            }
        };

        trace!("start_processing_loops unblocked, Cache Store was initialized");

        store.spawn_processing_loops()
    }

    pub async fn stop_processing_loops(&self) {
        let store = {
            let mut guard = self.state.write().await;
            match &*guard {
                LazyRocksCacheStoreState::Closed { .. } => {
                    return ();
                }
                LazyRocksCacheStoreState::FromRemote { .. } => {
                    *guard = LazyRocksCacheStoreState::Closed {};

                    return ();
                }
                LazyRocksCacheStoreState::Initialized { store } => {
                    let store_to_move = store.clone();

                    *guard = LazyRocksCacheStoreState::Closed {};

                    store_to_move
                }
            }
        };

        trace!("stop_processing_loops unblocked, Cache Store was initialized");

        store.stop_processing_loops().await
    }
}

#[async_trait]
impl CacheStore for LazyRocksCacheStore {
    async fn cache_all(&self, limit: Option<usize>) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        self.init().await?.cache_all(limit).await
    }

    async fn cache_set(
        &self,
        item: CacheItem,
        update_if_not_exists: bool,
    ) -> Result<bool, CubeError> {
        self.init()
            .await?
            .cache_set(item, update_if_not_exists)
            .await
    }

    async fn cache_truncate(&self) -> Result<(), CubeError> {
        self.init().await?.cache_truncate().await
    }

    async fn cache_delete(&self, key: String) -> Result<(), CubeError> {
        self.init().await?.cache_delete(key).await
    }

    async fn cache_get(&self, key: String) -> Result<Option<IdRow<CacheItem>>, CubeError> {
        self.init().await?.cache_get(key).await
    }

    async fn cache_keys(&self, prefix: String) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        self.init().await?.cache_keys(prefix).await
    }

    async fn cache_incr(&self, path: String) -> Result<IdRow<CacheItem>, CubeError> {
        self.init().await?.cache_incr(path).await
    }

    async fn queue_all(&self, limit: Option<usize>) -> Result<Vec<QueueAllItem>, CubeError> {
        self.init().await?.queue_all(limit).await
    }

    async fn queue_results_all(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<IdRow<QueueResult>>, CubeError> {
        self.init().await?.queue_results_all(limit).await
    }

    async fn queue_results_multi_delete(&self, ids: Vec<u64>) -> Result<(), CubeError> {
        self.init().await?.queue_results_multi_delete(ids).await
    }

    async fn queue_add(&self, payload: QueueAddPayload) -> Result<QueueAddResponse, CubeError> {
        self.init().await?.queue_add(payload).await
    }

    async fn queue_truncate(&self) -> Result<(), CubeError> {
        self.init().await?.queue_truncate().await
    }

    async fn queue_to_cancel(
        &self,
        prefix: String,
        orphaned_timeout: Option<u32>,
        heartbeat_timeout: Option<u32>,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        self.init()
            .await?
            .queue_to_cancel(prefix, orphaned_timeout, heartbeat_timeout)
            .await
    }

    async fn queue_list(
        &self,
        prefix: String,
        status_filter: Option<QueueItemStatus>,
        priority_sort: bool,
        with_payload: bool,
    ) -> Result<Vec<QueueListItem>, CubeError> {
        self.init()
            .await?
            .queue_list(prefix, status_filter, priority_sort, with_payload)
            .await
    }

    async fn queue_get(&self, key: QueueKey) -> Result<Option<QueueGetResponse>, CubeError> {
        self.init().await?.queue_get(key).await
    }

    async fn queue_cancel(&self, key: QueueKey) -> Result<Option<QueueCancelResponse>, CubeError> {
        self.init().await?.queue_cancel(key).await
    }

    async fn queue_heartbeat(&self, key: QueueKey) -> Result<(), CubeError> {
        self.init().await?.queue_heartbeat(key).await
    }

    async fn queue_retrieve_by_path(
        &self,
        path: String,
        allow_concurrency: u32,
    ) -> Result<QueueRetrieveResponse, CubeError> {
        self.init()
            .await?
            .queue_retrieve_by_path(path, allow_concurrency)
            .await
    }

    async fn queue_ack(&self, key: QueueKey, result: Option<String>) -> Result<bool, CubeError> {
        self.init().await?.queue_ack(key, result).await
    }

    async fn queue_result_by_path(
        &self,
        path: String,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        self.init().await?.queue_result_by_path(path).await
    }

    async fn queue_result_blocking(
        &self,
        key: QueueKey,
        timeout: u64,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        self.init().await?.queue_result_blocking(key, timeout).await
    }

    async fn queue_merge_extra(&self, key: QueueKey, payload: String) -> Result<(), CubeError> {
        self.init().await?.queue_merge_extra(key, payload).await
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        self.init().await?.compaction().await
    }

    async fn info(&self) -> Result<CachestoreInfo, CubeError> {
        self.init().await?.info().await
    }

    async fn eviction(&self) -> Result<EvictionResult, CubeError> {
        self.init().await?.eviction().await
    }

    async fn persist(&self) -> Result<(), CubeError> {
        self.init().await?.persist().await
    }

    async fn healthcheck(&self) -> Result<(), CubeError> {
        self.init().await?.healthcheck().await
    }

    async fn rocksdb_properties(&self) -> Result<Vec<RocksPropertyRow>, CubeError> {
        self.init().await?.rocksdb_properties().await
    }
}

crate::di_service!(LazyRocksCacheStore, [CacheStore]);
