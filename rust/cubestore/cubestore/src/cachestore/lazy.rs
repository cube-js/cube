use crate::cachestore::{CacheItem, CacheStore, RocksCacheStore};
use crate::config::ConfigObj;
use crate::metastore::{IdRow, MetaStoreFs};
use crate::CubeError;
use async_trait::async_trait;
use log::trace;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch::{Receiver, Sender};

pub enum LazyRocksCacheStoreState {
    FromRemote {
        path: String,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
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
}

impl LazyRocksCacheStore {
    pub async fn load_from_dump(
        path: &Path,
        dump_path: &Path,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        let store = RocksCacheStore::load_from_dump(path, dump_path, metastore_fs, config).await?;

        Ok(Arc::new(Self {
            init_signal: None,
            state: tokio::sync::RwLock::new(LazyRocksCacheStoreState::Initialized { store }),
        }))
    }

    pub async fn load_from_remote(
        path: &str,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        let (init_flag, init_signal) = tokio::sync::watch::channel::<bool>(false);

        Ok(Arc::new(Self {
            init_signal: Some(init_signal),
            state: tokio::sync::RwLock::new(LazyRocksCacheStoreState::FromRemote {
                path: path.to_string(),
                metastore_fs,
                config,
                init_flag,
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
                // receiver will be closed on drop
                init_flag: _,
            } => {
                let store =
                    RocksCacheStore::load_from_remote(&path, metastore_fs.clone(), config.clone())
                        .await?;

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

    pub async fn wait_upload_loop(&self) {
        if let Some(init_signal) = &self.init_signal {
            let _ = init_signal.clone().changed().await;
        }

        let store = {
            let guard = self.state.read().await;
            if let LazyRocksCacheStoreState::Initialized { store } = &*guard {
                store.clone()
            } else {
                return ();
            }
        };

        trace!("wait_upload_loop unblocked, Cache Store was initialized");

        store.wait_upload_loop().await
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
    async fn cache_all(&self) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        self.init().await?.cache_all().await
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

    async fn compaction(&self) -> Result<(), CubeError> {
        self.init().await?.compaction().await
    }
}

crate::di_service!(LazyRocksCacheStore, [CacheStore]);
