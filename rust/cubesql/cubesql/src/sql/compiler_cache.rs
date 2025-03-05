use crate::{
    compile::{
        engine::CubeContext,
        qtrace::Qtrace,
        rewrite::rewriter::{CubeEGraph, CubeRewrite, Rewriter},
        DatabaseProtocol,
    },
    config::ConfigObj,
    sql::AuthContextRef,
    transport::{MetaContext, TransportService},
    utils::egraph_hash,
    CubeError, MutexAsync, RWLockAsync,
};
use async_trait::async_trait;
use datafusion::scalar::ScalarValue;
use lru::LruCache;
use std::{collections::HashMap, fmt::Debug, num::NonZeroUsize, sync::Arc};
use uuid::Uuid;

#[async_trait]
pub trait CompilerCache: Send + Sync + Debug {
    async fn rewrite_rules(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
        eval_stable_functions: bool,
    ) -> Result<Arc<Vec<CubeRewrite>>, CubeError>;

    async fn meta(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
    ) -> Result<Arc<MetaContext>, CubeError>;

    async fn parameterized_rewrite(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
        cube_context: Arc<CubeContext>,
        input_plan: CubeEGraph,
        qtrace: &mut Option<Qtrace>,
    ) -> Result<CubeEGraph, CubeError>;

    async fn rewrite(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
        cube_context: Arc<CubeContext>,
        input_plan: CubeEGraph,
        param_values: &HashMap<usize, ScalarValue>,
        qtrace: &mut Option<Qtrace>,
    ) -> Result<CubeEGraph, CubeError>;

    async fn get_cache_entry(
        &self,
        ctx: AuthContextRef,
        protocol: DatabaseProtocol,
    ) -> Result<Arc<CompilerCacheEntry>, CubeError>;
}

#[derive(Debug)]
pub struct CompilerCacheImpl {
    config_obj: Arc<dyn ConfigObj>,
    transport: Arc<dyn TransportService>,
    compiler_id_to_entry: MutexAsync<LruCache<(Uuid, DatabaseProtocol), Arc<CompilerCacheEntry>>>,
}

pub struct CompilerCacheEntry {
    meta_context: Arc<MetaContext>,
    rewrite_rules: RWLockAsync<HashMap<bool, Arc<Vec<CubeRewrite>>>>,
    parameterized_cache: MutexAsync<LruCache<[u8; 32], CubeEGraph>>,
    queries_cache: MutexAsync<LruCache<[u8; 32], CubeEGraph>>,
}

crate::di_service!(CompilerCacheImpl, [CompilerCache]);

#[async_trait]
impl CompilerCache for CompilerCacheImpl {
    async fn rewrite_rules(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
        eval_stable_functions: bool,
    ) -> Result<Arc<Vec<CubeRewrite>>, CubeError> {
        let rewrite_rules = {
            cache_entry
                .rewrite_rules
                .read()
                .await
                .get(&eval_stable_functions)
                .cloned()
        };
        if let Some(rewrite_rules) = rewrite_rules {
            Ok(rewrite_rules)
        } else {
            let mut rewrite_rules_lock = cache_entry.rewrite_rules.write().await;
            if let Some(rewrite_rules) = rewrite_rules_lock.get(&eval_stable_functions).cloned() {
                Ok(rewrite_rules)
            } else {
                let rewrite_rules = Arc::new(Rewriter::rewrite_rules(
                    cache_entry.meta_context.clone(),
                    self.config_obj.clone(),
                    eval_stable_functions,
                ));

                rewrite_rules_lock.insert(eval_stable_functions, rewrite_rules.clone());
                Ok(rewrite_rules)
            }
        }
    }

    async fn meta(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
    ) -> Result<Arc<MetaContext>, CubeError> {
        Ok(cache_entry.meta_context.clone())
    }

    async fn parameterized_rewrite(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
        cube_context: Arc<CubeContext>,
        parameterized_graph: CubeEGraph,
        qtrace: &mut Option<Qtrace>,
    ) -> Result<CubeEGraph, CubeError> {
        let graph_key = egraph_hash(&parameterized_graph, None);

        let cache_entry_clone = Arc::clone(&cache_entry);
        let mut rewrites_cache_lock = cache_entry.parameterized_cache.lock().await;
        if let Some(rewrite_entry) = rewrites_cache_lock.get(&graph_key) {
            Ok(rewrite_entry.clone())
        } else {
            let mut rewriter = Rewriter::new(parameterized_graph, cube_context);
            let rewrite_entry = rewriter
                .run_rewrite_to_completion(cache_entry_clone, qtrace)
                .await?;
            rewrites_cache_lock.put(graph_key, rewrite_entry.clone());
            Ok(rewrite_entry)
        }
    }

    async fn rewrite(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
        cube_context: Arc<CubeContext>,
        input_plan: CubeEGraph,
        param_values: &HashMap<usize, ScalarValue>,
        qtrace: &mut Option<Qtrace>,
    ) -> Result<CubeEGraph, CubeError> {
        if !self.config_obj.enable_rewrite_cache() {
            let mut rewriter = Rewriter::new(input_plan, cube_context);
            rewriter.add_param_values(param_values)?;
            return Ok(rewriter
                .run_rewrite_to_completion(cache_entry, qtrace)
                .await?);
        }

        let graph_key = egraph_hash(&input_plan, Some(param_values));

        let cache_entry_clone = Arc::clone(&cache_entry);
        let mut rewrites_cache_lock = cache_entry.queries_cache.lock().await;
        if let Some(plan) = rewrites_cache_lock.get(&graph_key) {
            Ok(plan.clone())
        } else {
            let graph = if self.config_obj.enable_parameterized_rewrite_cache() {
                self.parameterized_rewrite(
                    Arc::clone(&cache_entry),
                    cube_context.clone(),
                    input_plan,
                    qtrace,
                )
                .await?
            } else {
                input_plan
            };
            let mut rewriter = Rewriter::new(graph, cube_context);
            rewriter.add_param_values(param_values)?;
            let final_plan = rewriter
                .run_rewrite_to_completion(cache_entry_clone, qtrace)
                .await?;
            rewrites_cache_lock.put(graph_key, final_plan.clone());
            Ok(final_plan)
        }
    }

    async fn get_cache_entry(
        &self,
        ctx: AuthContextRef,
        protocol: DatabaseProtocol,
    ) -> Result<Arc<CompilerCacheEntry>, CubeError> {
        let compiler_id = self.transport.compiler_id(ctx.clone()).await?;
        let cache_entry = {
            self.compiler_id_to_entry
                .lock()
                .await
                .get(&(compiler_id, protocol.clone()))
                .cloned()
        };
        // Double checked locking
        let cache_entry = if let Some(cache_entry) = cache_entry {
            cache_entry
        } else {
            let meta_context = self.transport.meta(ctx.clone()).await?;
            let mut compiler_id_to_entry = self.compiler_id_to_entry.lock().await;
            compiler_id_to_entry
                .get(&(meta_context.compiler_id, protocol.clone()))
                .cloned()
                .unwrap_or_else(|| {
                    let cache_entry = Arc::new(CompilerCacheEntry {
                        meta_context: meta_context.clone(),
                        rewrite_rules: RWLockAsync::new(HashMap::new()),
                        parameterized_cache: MutexAsync::new(LruCache::new(
                            NonZeroUsize::new(self.config_obj.query_cache_size()).unwrap(),
                        )),
                        queries_cache: MutexAsync::new(LruCache::new(
                            NonZeroUsize::new(self.config_obj.query_cache_size()).unwrap(),
                        )),
                    });
                    compiler_id_to_entry.put(
                        (meta_context.compiler_id, protocol.clone()),
                        cache_entry.clone(),
                    );
                    cache_entry
                })
        };
        Ok(cache_entry)
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
}
