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
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub enum LazyRocksCacheStoreState {
    FromRemote {
        #[allow(dead_code)] // Receiver notified when this sender is dropped on transition
        init_flag: Sender<bool>,
    },
    Closed {},
    Initialized {
        store: Arc<RocksCacheStore>,
    },
}

pub struct LazyRocksCacheStore {
    init_signal: Option<Receiver<bool>>,
    state: tokio::sync::RwLock<LazyRocksCacheStoreState>,
    // Kept on the wrapper (not only inside the state enum) so that `wipe` can rebuild the store
    // from scratch regardless of the current state.
    path: String,
    metastore_fs: Arc<dyn MetaStoreFs>,
    config: Arc<dyn ConfigObj>,
    listeners: Vec<tokio::sync::broadcast::Sender<MetaStoreEvent>>,
    // Handles of the currently running processing loops. Owned here (rather than by CubeServices)
    // so that `wipe` can stop and join them before dropping/reopening the RocksDB.
    running_loops: Mutex<Vec<JoinHandle<Result<(), CubeError>>>>,
    shutdown_token: CancellationToken,
}

impl LazyRocksCacheStore {
    pub async fn load_from_dump(
        path: &Path,
        dump_path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        listeners: Vec<tokio::sync::broadcast::Sender<MetaStoreEvent>>,
    ) -> Result<Arc<Self>, CubeError> {
        let store =
            RocksCacheStore::load_from_dump(path, dump_path, metastore_fs.clone(), config.clone())
                .await?;

        for listener in &listeners {
            store.add_listener(listener.clone()).await;
        }

        Ok(Arc::new(Self {
            init_signal: None,
            state: tokio::sync::RwLock::new(LazyRocksCacheStoreState::Initialized { store }),
            path: path.to_string_lossy().to_string(),
            metastore_fs,
            config,
            listeners,
            running_loops: Mutex::new(vec![]),
            shutdown_token: CancellationToken::new(),
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
            state: tokio::sync::RwLock::new(LazyRocksCacheStoreState::FromRemote { init_flag }),
            path: path.to_string(),
            metastore_fs,
            config,
            listeners,
            running_loops: Mutex::new(vec![]),
            shutdown_token: CancellationToken::new(),
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
            LazyRocksCacheStoreState::FromRemote { .. } => {
                let store = RocksCacheStore::load_from_remote(
                    &self.path,
                    self.metastore_fs.clone(),
                    self.config.clone(),
                )
                .await?;

                for listener in &self.listeners {
                    store.add_listener(listener.clone()).await;
                }

                // Dropping the previous FromRemote (via replace) drops init_flag, which
                // notifies run_processing_loops to spawn the processing loops.
                *guard = LazyRocksCacheStoreState::Initialized {
                    store: store.clone(),
                };

                Ok(store)
            }
            // Another caller initialized it between our read and write lock.
            LazyRocksCacheStoreState::Initialized { store } => Ok(store.clone()),
            LazyRocksCacheStoreState::Closed { .. } => Err(CubeError::internal(
                "Unable to initialize Cache Store on lazy call, it was closed".to_string(),
            )),
        }
    }

    /// Owns the processing-loop lifecycle for the cachestore. Called once by CubeServices; it
    /// waits until the store is initialized, spawns the processing loops (stored in
    /// `running_loops` so `wipe` can manage them), then blocks until shutdown.
    pub async fn run_processing_loops(self: Arc<Self>) -> Result<(), CubeError> {
        if let Some(init_signal) = &self.init_signal {
            let _ = init_signal.clone().changed().await;
        }

        {
            let store = {
                let guard = self.state.read().await;
                if let LazyRocksCacheStoreState::Initialized { store } = &*guard {
                    Some(store.clone())
                } else {
                    None
                }
            };

            if let Some(store) = store {
                let mut loops = self.running_loops.lock().await;
                if loops.is_empty() {
                    *loops = store.spawn_processing_loops();
                }
                // `store` clone is dropped at the end of this block so it does not keep the
                // RocksDB alive while we are parked on the shutdown token.
            }
        }

        self.shutdown_token.cancelled().await;

        Ok(())
    }

    pub async fn stop_processing_loops(&self) {
        let store = {
            let mut guard = self.state.write().await;
            match &*guard {
                LazyRocksCacheStoreState::Closed { .. } => None,
                LazyRocksCacheStoreState::FromRemote { .. } => {
                    *guard = LazyRocksCacheStoreState::Closed {};
                    None
                }
                LazyRocksCacheStoreState::Initialized { store } => {
                    let store_to_move = store.clone();
                    *guard = LazyRocksCacheStoreState::Closed {};
                    Some(store_to_move)
                }
            }
        };

        if let Some(store) = store {
            store.stop_processing_loops().await;
        }

        {
            let mut loops = self.running_loops.lock().await;
            for handle in loops.drain(..) {
                let _ = handle.await;
            }
        }

        self.shutdown_token.cancel();
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
        caller_process_id: Option<String>,
    ) -> Result<Vec<QueueListItem>, CubeError> {
        self.init()
            .await?
            .queue_list(
                prefix,
                status_filter,
                priority_sort,
                with_payload,
                caller_process_id,
            )
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
        caller_process_id: Option<String>,
    ) -> Result<QueueRetrieveResponse, CubeError> {
        self.init()
            .await?
            .queue_retrieve_by_path(path, allow_concurrency, caller_process_id)
            .await
    }

    async fn queue_ack(&self, key: QueueKey, result: Option<String>) -> Result<bool, CubeError> {
        self.init().await?.queue_ack(key, result).await
    }

    async fn queue_result(
        &self,
        key: QueueKey,
        external_id: Option<String>,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        self.init().await?.queue_result(key, external_id).await
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

    async fn wipe(&self) -> Result<(), CubeError> {
        // Make sure the store is initialized. init() fires init_signal so run_processing_loops
        // spawns the initial loops; the returned Arc is dropped immediately so it does not
        // inflate the strong count we rely on below.
        self.init().await?;

        // Hold the state write lock for the whole teardown: new operations block on
        // state.read() in init() until we install the fresh store.
        let mut guard = self.state.write().await;
        let store = match std::mem::replace(&mut *guard, LazyRocksCacheStoreState::Closed {}) {
            LazyRocksCacheStoreState::Initialized { store } => store,
            LazyRocksCacheStoreState::Closed {} => {
                return Err(CubeError::internal(
                    "Unable to wipe Cache Store, it was closed".to_string(),
                ));
            }
            LazyRocksCacheStoreState::FromRemote { init_flag } => {
                // init() above guarantees Initialized; restore defensively.
                *guard = LazyRocksCacheStoreState::FromRemote { init_flag };
                return Err(CubeError::internal(
                    "Unable to wipe Cache Store, unexpected state".to_string(),
                ));
            }
        };

        // Stop the worker loops and JOIN them so their Arc<RocksCacheStore> clones are released.
        store.stop_processing_loops().await;
        {
            let mut loops = self.running_loops.lock().await;
            for handle in loops.drain(..) {
                let _ = handle.await;
            }
        }

        // Flush both RW loops so no in-flight operation still holds an Arc<DB> clone.
        store.drain_rw_loops().await?;

        // Wait until we are the sole owner of the store before closing it, so RocksDB releases
        // the directory LOCK and the reopen below succeeds.
        let deadline = Instant::now() + Duration::from_secs(30);
        while Arc::strong_count(&store) > 1 {
            if Instant::now() >= deadline {
                // Restore the store so the cachestore stays usable instead of corrupting it.
                let mut loops = self.running_loops.lock().await;
                *loops = store.clone().spawn_processing_loops();
                *guard = LazyRocksCacheStoreState::Initialized { store };
                return Err(CubeError::internal(
                    "Unable to wipe Cache Store: store is still in use after draining loops"
                        .to_string(),
                ));
            }

            tokio::task::yield_now().await;
        }

        // Close the DB (drops the last Arc<DB>) and drop the folder.
        drop(store);

        if let Err(err) = std::fs::remove_dir_all(&self.path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(CubeError::internal(format!(
                    "Unable to wipe Cache Store: failed to remove {}: {}",
                    self.path, err
                )));
            }
        }

        // Reopen from scratch (create_if_missing, no remote download), run migrations and
        // re-attach listeners.
        let fresh = RocksCacheStore::new(
            Path::new(&self.path),
            self.metastore_fs.clone(),
            self.config.clone(),
        )?;
        fresh.check_all_indexes().await?;
        for listener in &self.listeners {
            fresh.add_listener(listener.clone()).await;
        }

        // Persist a fresh full snapshot and move the remote cachestore-current pointer onto it,
        // superseding the poisoned lineage.
        fresh.upload_check_point().await?;

        // Respawn the worker loops against the fresh store.
        {
            let mut loops = self.running_loops.lock().await;
            *loops = fresh.clone().spawn_processing_loops();
        }

        *guard = LazyRocksCacheStoreState::Initialized { store: fresh };

        Ok(())
    }
}

crate::di_service!(LazyRocksCacheStore, [CacheStore]);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{init_test_logger, Config};
    use crate::metastore::BaseRocksStoreFs;
    use crate::remotefs::LocalDirRemoteFs;
    use std::env;
    use std::path::PathBuf;

    fn test_dirs(test_name: &str) -> (PathBuf, PathBuf) {
        let base = env::current_dir().unwrap().join("db-tmp").join("tests");
        (
            base.join(format!("{}-local", test_name)),
            base.join(format!("{}-remote", test_name)),
        )
    }

    async fn build_lazy(
        config: Config,
        local_path: PathBuf,
        remote_path: PathBuf,
    ) -> Arc<LazyRocksCacheStore> {
        let config_obj = config.config_obj();
        let remote_fs = LocalDirRemoteFs::new(Some(remote_path), local_path.clone());
        let cachestore_path = local_path.join("cachestore");
        let metastore_fs = BaseRocksStoreFs::new_for_cachestore(remote_fs, config_obj.clone());

        LazyRocksCacheStore::load_from_remote(
            cachestore_path.to_str().unwrap(),
            metastore_fs,
            config_obj,
            vec![],
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_lazy_wipe_clears_state_and_stays_usable() -> Result<(), CubeError> {
        init_test_logger().await;

        let (local, remote) = test_dirs("lazy_wipe_clears");
        let _ = std::fs::remove_dir_all(&local);
        let _ = std::fs::remove_dir_all(&remote);

        let cachestore = build_lazy(Config::test("lazy_wipe_clears"), local.clone(), remote).await;

        cachestore
            .cache_set(
                CacheItem::new("prefix:key1".to_string(), Some(60), "v1".to_string()),
                false,
            )
            .await?;
        cachestore
            .cache_set(
                CacheItem::new("prefix:key2".to_string(), Some(60), "v2".to_string()),
                false,
            )
            .await?;
        cachestore
            .queue_add(QueueAddPayload {
                path: "queue:job1".to_string(),
                value: "payload".to_string(),
                priority: 0,
                orphaned: None,
                process_id: None,
                exclusive: false,
                external_id: None,
            })
            .await?;

        assert_eq!(cachestore.cache_all(None).await?.len(), 2);
        assert_eq!(cachestore.queue_all(None).await?.len(), 1);

        cachestore.wipe().await?;

        assert_eq!(cachestore.cache_all(None).await?.len(), 0);
        assert_eq!(cachestore.queue_all(None).await?.len(), 0);

        // The reopened store must still be usable.
        cachestore
            .cache_set(
                CacheItem::new("prefix:after".to_string(), Some(60), "fresh".to_string()),
                false,
            )
            .await?;
        let row = cachestore
            .cache_get("prefix:after".to_string())
            .await?
            .expect("must return row after wipe");
        assert_eq!(row.into_row().value, "fresh".to_string());

        // The local cachestore folder must exist again after the reopen.
        assert!(local.join("cachestore").exists());

        let _ = std::fs::remove_dir_all(&local);

        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_wipe_publishes_clean_remote_snapshot() -> Result<(), CubeError> {
        init_test_logger().await;

        let (local, remote) = test_dirs("lazy_wipe_remote");
        let _ = std::fs::remove_dir_all(&local);
        let _ = std::fs::remove_dir_all(&remote);

        let cachestore = build_lazy(
            Config::test("lazy_wipe_remote"),
            local.clone(),
            remote.clone(),
        )
        .await;

        cachestore
            .cache_set(
                CacheItem::new("prefix:key1".to_string(), Some(60), "v1".to_string()),
                false,
            )
            .await?;

        cachestore.wipe().await?;

        // A fresh instance pointing at the same remote (with an empty local dir) must download
        // the post-wipe snapshot via cachestore-current and come back empty.
        let local2 = local.parent().unwrap().join("lazy_wipe_remote-local2");
        let _ = std::fs::remove_dir_all(&local2);

        let reloaded = build_lazy(Config::test("lazy_wipe_remote"), local2.clone(), remote).await;
        assert_eq!(reloaded.cache_all(None).await?.len(), 0);

        let _ = std::fs::remove_dir_all(&local);
        let _ = std::fs::remove_dir_all(&local2);

        Ok(())
    }
}
