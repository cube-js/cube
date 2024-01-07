use crate::{
    compile::rewrite::{analysis::LogicalPlanAnalysis, rewriter::Rewriter, LogicalPlanLanguage},
    config::ConfigObj,
    sql::AuthContextRef,
    transport::{MetaContext, TransportService},
    CubeError, MutexAsync, RWLockAsync,
};
use async_trait::async_trait;
use egg::Rewrite;
use lru::LruCache;
use std::{fmt::Debug, num::NonZeroUsize, sync::Arc};
use uuid::Uuid;

#[async_trait]
pub trait CompilerCache: Send + Sync + Debug {
    async fn rewrite_rules(
        &self,
        ctx: AuthContextRef,
    ) -> Result<Arc<Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>>, CubeError>;

    async fn meta(&self, ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError>;
}

#[derive(Debug)]
pub struct CompilerCacheImpl {
    config_obj: Arc<dyn ConfigObj>,
    transport: Arc<dyn TransportService>,
    compiler_id_to_entry: MutexAsync<LruCache<Uuid, Arc<CompilerCacheEntry>>>,
}

pub struct CompilerCacheEntry {
    meta_context: Arc<MetaContext>,
    rewrite_rules: RWLockAsync<Option<Arc<Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>>>>,
}

crate::di_service!(CompilerCacheImpl, [CompilerCache]);

#[async_trait]
impl CompilerCache for CompilerCacheImpl {
    async fn rewrite_rules(
        &self,
        ctx: AuthContextRef,
    ) -> Result<Arc<Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>>, CubeError> {
        let cache_entry = self.get_cache_entry(ctx.clone()).await?;

        let rewrite_rules = { cache_entry.rewrite_rules.read().await.clone() };
        if let Some(rewrite_rules) = rewrite_rules {
            Ok(rewrite_rules)
        } else {
            let mut rewrite_rules_lock = cache_entry.rewrite_rules.write().await;
            if let Some(rewrite_rules) = rewrite_rules_lock.clone() {
                Ok(rewrite_rules)
            } else {
                let rewrite_rules = Arc::new(Rewriter::rewrite_rules(
                    cache_entry.meta_context.clone(),
                    self.config_obj.clone(),
                ));
                *rewrite_rules_lock = Some(rewrite_rules.clone());
                Ok(rewrite_rules)
            }
        }
    }

    async fn meta(&self, ctx: AuthContextRef) -> Result<Arc<MetaContext>, CubeError> {
        let cache_entry = self.get_cache_entry(ctx.clone()).await?;
        Ok(cache_entry.meta_context.clone())
    }
}

impl CompilerCacheImpl {
    pub fn new(config_obj: Arc<dyn ConfigObj>, transport: Arc<dyn TransportService>) -> Self {
        let compiler_cache_size = config_obj.compiler_cache_size();
        CompilerCacheImpl {
            config_obj,
            transport,
            compiler_id_to_entry: MutexAsync::new(LruCache::new(
                NonZeroUsize::new(compiler_cache_size).unwrap(),
            )),
        }
    }

    pub async fn get_cache_entry(
        &self,
        ctx: AuthContextRef,
    ) -> Result<Arc<CompilerCacheEntry>, CubeError> {
        let compiler_id = self.transport.compiler_id(ctx.clone()).await?;
        let cache_entry = {
            self.compiler_id_to_entry
                .lock()
                .await
                .get(&compiler_id)
                .cloned()
        };
        // Double checked locking
        let cache_entry = if let Some(cache_entry) = cache_entry {
            cache_entry
        } else {
            let meta_context = self.transport.meta(ctx.clone()).await?;
            let mut compiler_id_to_entry = self.compiler_id_to_entry.lock().await;
            compiler_id_to_entry
                .get(&meta_context.compiler_id)
                .cloned()
                .unwrap_or_else(|| {
                    let cache_entry = Arc::new(CompilerCacheEntry {
                        meta_context: meta_context.clone(),
                        rewrite_rules: RWLockAsync::new(None),
                    });
                    compiler_id_to_entry.put(meta_context.compiler_id.clone(), cache_entry.clone());
                    cache_entry
                })
        };
        Ok(cache_entry)
    }
}
