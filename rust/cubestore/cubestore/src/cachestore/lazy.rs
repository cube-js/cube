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
use datafusion::cube_ext;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const WIPE_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const WIPE_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);
const WIPE_CLOSE_POLL_INTERVAL: Duration = Duration::from_millis(20);
const WIPE_REOPEN_TIMEOUT: Duration = Duration::from_secs(10);

fn is_rocksdb_lock_error(err: &CubeError) -> bool {
    let msg = err.to_string();
    msg.contains("lock hold by current process")
        || msg.contains("While lock file")
        || msg.contains("Resource temporarily unavailable")
}

pub enum LazyRocksCacheStoreState {
    FromRemote {
        #[allow(dead_code)] // Receiver notified when this sender is dropped on transition
        init_flag: Sender<bool>,
    },
    Closed {},
    Wiping {},
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
                LazyRocksCacheStoreState::Wiping { .. } => {
                    return Err(CubeError::internal(
                        "Unable to initialize Cache Store on lazy call, a wipe is in progress"
                            .to_string(),
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
            LazyRocksCacheStoreState::Wiping { .. } => Err(CubeError::internal(
                "Unable to initialize Cache Store on lazy call, a wipe is in progress".to_string(),
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
                LazyRocksCacheStoreState::Wiping { .. } => {
                    // A wipe owns the store; marking Closed makes the wipe abort its re-install
                    // (see wipe) instead of resurrecting the store after shutdown was requested.
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
        // inflate the strong count the teardown relies on below.
        self.init().await?;

        {
            let mut guard = self.state.write().await;
            match std::mem::replace(&mut *guard, LazyRocksCacheStoreState::Wiping {}) {
                LazyRocksCacheStoreState::Initialized { store } => {
                    // Drop the guard before the teardown so the lock is not held across it.
                    drop(guard);

                    // Everything from here is the point of no return: the loops are stopped and
                    // the store is destroyed, so it cannot be restored.
                    if let Err(err) = self.wipe_teardown(store).await {
                        let mut guard = self.state.write().await;
                        *guard = LazyRocksCacheStoreState::Closed {};
                        drop(guard);

                        if self.shutdown_token.is_cancelled() {
                            log::error!(
                                "SYSTEM CACHESTORE WIPE aborted during shutdown; cachestore left \
                                 closed: {}",
                                err
                            );
                        } else {
                            log::error!(
                                "SYSTEM CACHESTORE WIPE failed after the teardown point of no \
                                 return; cachestore is now CLOSED and will reject every operation \
                                 until the node is restarted: {}",
                                err
                            );
                        }

                        return Err(err);
                    }

                    Ok(())
                }
                LazyRocksCacheStoreState::Wiping {} => {
                    *guard = LazyRocksCacheStoreState::Wiping {};
                    Err(CubeError::internal(
                        "Unable to wipe Cache Store: a wipe is already in progress".to_string(),
                    ))
                }
                LazyRocksCacheStoreState::Closed {} => {
                    *guard = LazyRocksCacheStoreState::Closed {};
                    Err(CubeError::internal(
                        "Unable to wipe Cache Store, it was closed".to_string(),
                    ))
                }
                LazyRocksCacheStoreState::FromRemote { init_flag } => {
                    // init() above guarantees Initialized; restore defensively.
                    *guard = LazyRocksCacheStoreState::FromRemote { init_flag };
                    Err(CubeError::internal(
                        "Unable to wipe Cache Store, unexpected state".to_string(),
                    ))
                }
            }
        }
    }
}

impl LazyRocksCacheStore {
    async fn wipe_teardown(&self, store: Arc<RocksCacheStore>) -> Result<(), CubeError> {
        // Stop the worker loops and JOIN them so their Arc<RocksCacheStore> clones are released.
        // WorkerLoop/IntervalLoop tokens are permanently cancelled, so the old `store` can no
        // longer run loops and there is no restore path.
        store.stop_processing_loops().await;
        {
            let mut loops = self.running_loops.lock().await;
            for handle in loops.drain(..) {
                let _ = handle.await;
            }
        }

        // Flush both RW loops so in-flight serialized ops release their transient Arc<DB> clones.
        // Bounded so a wedged RW loop cannot hang wipe forever.
        if let Err(err) = tokio::time::timeout(WIPE_DRAIN_TIMEOUT, store.drain_rw_loops()).await {
            log::warn!(
                "Wiping cachestore: draining RW loops timed out ({:?}), continuing: {}",
                WIPE_DRAIN_TIMEOUT,
                err
            );
        }

        // Wait until nothing else references the store or the underlying RocksDB, so that
        // drop(store) closes the DB (releasing the directory LOCK) before we reopen. We watch
        // the real Arc<DB> via db_strong_count() to catch detached out-of-queue spawn_blocking
        // readers that the RW-loop drain above does not cover. Sleep-backoff, not a busy spin.
        let deadline = Instant::now() + WIPE_CLOSE_TIMEOUT;

        while Arc::strong_count(&store) > 1 || store.db_strong_count() > 1 {
            if Instant::now() >= deadline {
                // We already committed (loops stopped); the store cannot be restored cleanly.
                // Bail rather than risk removing/reopening the directory while the old DB is still
                // open (which could corrupt the new DB); the caller marks the state Closed.
                return Err(CubeError::internal(
                    "Unable to wipe Cache Store: still in use after draining; restart the node"
                        .to_string(),
                ));
            }

            log::warn!(
                "Wiping cachestore: waiting for outstanding Cache Store references to be released before closing the DB (store strong count: {}, DB strong count: {})",
                Arc::strong_count(&store),
                store.db_strong_count()
            );

            tokio::time::sleep(WIPE_CLOSE_POLL_INTERVAL).await;
        }

        // Close the DB (drops the last Arc<DB>) synchronously.
        drop(store);

        // Remove the folder and reopen from scratch off the async executor (blocking syscalls +
        // RocksDB open). Retry the open on a held directory LOCK to cover any residual detached
        // Arc<DB> that outlived the wait above.
        let path = self.path.clone();
        let metastore_fs = self.metastore_fs.clone();
        let config = self.config.clone();

        let fresh = cube_ext::spawn_blocking(move || -> Result<Arc<RocksCacheStore>, CubeError> {
            if let Err(err) = std::fs::remove_dir_all(&path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(CubeError::internal(format!(
                        "Unable to wipe Cache Store: failed to remove {}: {}",
                        path, err
                    )));
                }
            }

            let reopen_deadline = Instant::now() + WIPE_REOPEN_TIMEOUT;

            loop {
                match RocksCacheStore::new(Path::new(&path), metastore_fs.clone(), config.clone()) {
                    Ok(store) => return Ok(store),
                    Err(err) if is_rocksdb_lock_error(&err) && Instant::now() < reopen_deadline => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(err) => return Err(err),
                }
            }
        })
        .await??;

        fresh.check_all_indexes().await?;

        for listener in &self.listeners {
            fresh.add_listener(listener.clone()).await;
        }

        {
            let mut guard = self.state.write().await;

            // Respawn the worker loops against the fresh store, then install it and release the
            // state lock BEFORE the (remote, slow, fallible) snapshot upload.
            {
                let mut loops = self.running_loops.lock().await;
                *loops = fresh.clone().spawn_processing_loops();
            }

            *guard = LazyRocksCacheStoreState::Initialized {
                store: fresh.clone(),
            };
        }

        // Persist a fresh full snapshot and move the remote cachestore-current pointer onto it.
        // Best-effort: the store is already installed and usable, and the upload loop retries;
        // a transient remote error must not brick the cachestore.
        if let Err(err) = fresh.upload_check_point().await {
            log::warn!(
                "Wiped cachestore reopened, but persisting the fresh snapshot failed (upload loop will retry): {}",
                err
            );
        }

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

        let cachestore = build_lazy(
            Config::test("lazy_wipe_clears"),
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

        // Stop the loops respawned by wipe so the test does not leak background tasks.
        cachestore.stop_processing_loops().await;

        let _ = std::fs::remove_dir_all(&local);
        let _ = std::fs::remove_dir_all(&remote);

        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_wipe_publishes_clean_remote_snapshot() -> Result<(), CubeError> {
        init_test_logger().await;

        let (local, remote) = test_dirs("lazy_wipe_remote");
        let local_pre = local.parent().unwrap().join("lazy_wipe_remote-local-pre");
        let local2 = local.parent().unwrap().join("lazy_wipe_remote-local2");
        for dir in [&local, &remote, &local_pre, &local2] {
            let _ = std::fs::remove_dir_all(dir);
        }

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

        // Upload the seed to the remote so the test actually proves that wipe RESETS the remote
        // (rather than that an empty remote yields an empty store). init() returns the inner
        // store; the returned Arc is scoped so it is dropped before wipe (else it would inflate
        // the strong count wipe waits on).
        {
            let store = cachestore.init().await?;
            store.upload_check_point().await?;
        }

        // A fresh instance over the same remote must now see the seeded row (pre-wipe state).
        {
            let reloaded = build_lazy(
                Config::test("lazy_wipe_remote"),
                local_pre.clone(),
                remote.clone(),
            )
            .await;
            assert_eq!(reloaded.cache_all(None).await?.len(), 1);
            reloaded.stop_processing_loops().await;
        }

        cachestore.wipe().await?;

        // After wipe, a fresh instance over the same remote must download the post-wipe snapshot
        // via cachestore-current and come back empty.
        let reloaded = build_lazy(
            Config::test("lazy_wipe_remote"),
            local2.clone(),
            remote.clone(),
        )
        .await;
        assert_eq!(reloaded.cache_all(None).await?.len(), 0);
        reloaded.stop_processing_loops().await;

        cachestore.stop_processing_loops().await;

        for dir in [&local, &remote, &local_pre, &local2] {
            let _ = std::fs::remove_dir_all(dir);
        }

        Ok(())
    }
}
