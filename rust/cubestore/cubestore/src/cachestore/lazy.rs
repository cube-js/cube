use crate::cachestore::{CacheItem, CacheStore, RocksCacheStore};
use crate::config::ConfigObj;
use crate::metastore::{IdRow, MetaStoreFs};
use crate::CubeError;
use async_trait::async_trait;
use log::trace;
use std::borrow::Borrow;
use std::sync::Arc;
use tokio::sync::watch::{Receiver, Sender};

pub enum LazyRocksCacheStoreState {
    FromRemote {
        path: String,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
        init_flag: Sender<bool>,
    },
    Initializing {},
    Initialized {
        store: Arc<RocksCacheStore>,
    },
}

pub struct LazyRocksCacheStore {
    init_signal: Receiver<bool>,
    state: tokio::sync::RwLock<LazyRocksCacheStoreState>,
}

impl LazyRocksCacheStore {
    pub async fn load_from_remote(
        path: &str,
        metastore_fs: Arc<dyn MetaStoreFs>,
        config: Arc<dyn ConfigObj>,
    ) -> Result<Arc<Self>, CubeError> {
        let (init_flag, init_signal) = tokio::sync::watch::channel::<bool>(false);

        Ok(Arc::new(Self {
            init_signal,
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
            if let LazyRocksCacheStoreState::Initialized { store } = &*guard {
                return Ok(store.clone());
            };
        }

        let mut guard = self.state.write().await;
        let state = std::mem::replace(&mut *guard, LazyRocksCacheStoreState::Initializing {});
        match state {
            LazyRocksCacheStoreState::FromRemote {
                path,
                metastore_fs,
                config,
                init_flag,
            } => {
                let store = RocksCacheStore::load_from_remote(&path, metastore_fs, config).await?;

                *guard = LazyRocksCacheStoreState::Initialized {
                    store: store.clone(),
                };

                init_flag.send(true)?;

                Ok(store)
            }
            _ => Err(CubeError::internal(
                "Unable to initialize Cache Store on lazy call, unexpected state".to_string(),
            )),
        }
    }

    async fn get_initialized_store(&self) -> Result<Arc<RocksCacheStore>, CubeError> {
        let guard = self.state.read().await;
        if let LazyRocksCacheStoreState::Initialized { store } = &*guard {
            Ok(store.clone())
        } else {
            Err(CubeError::internal(
                "Unable to extract store from the state".to_string(),
            ))
        }
    }

    pub async fn wait_upload_loop(&self) {
        self.init_signal
            .clone()
            .changed()
            .await
            .expect("Everything is fine");

        trace!("wait_upload_loop unblocked, Cache Store was initialized");

        self.get_initialized_store()
            .await
            .unwrap()
            .wait_upload_loop();
    }

    pub async fn stop_processing_loops(&self) {
        self.init_signal
            .clone()
            .changed()
            .await
            .expect("Everything is fine");

        trace!("stop_processing_loops unblocked, Cache Store was initialized");

        self.get_initialized_store()
            .await
            .unwrap()
            .stop_processing_loops();
    }
}

#[async_trait]
impl CacheStore for LazyRocksCacheStore {
    async fn cache_incr(&self, path: String) -> Result<IdRow<CacheItem>, CubeError> {
        self.init().await?.cache_incr(path).await
    }

    async fn cache_all(&self) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        self.init().await?.cache_all().await
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        self.init().await?.compaction().await
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
}

crate::di_service!(LazyRocksCacheStore, [CacheStore]);
