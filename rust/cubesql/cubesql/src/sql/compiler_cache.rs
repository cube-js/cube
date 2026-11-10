use crate::{
    compile::{
        engine::CubeContext,
        qtrace::Qtrace,
        rewrite::rewriter::{CubeEGraph, CubeRewrite, Rewriter},
        DatabaseProtocol,
    },
    config::ConfigObj,
    sql::AuthContextRef,
    transport::{
        LoadRequestMeta, MetaContext, SpanId, TransportLoadRequestQuery, TransportService,
    },
    utils::egraph_hash,
    CubeError, MutexAsync, RWLockAsync,
};
use async_trait::async_trait;
use datafusion::scalar::ScalarValue;
use lru::LruCache;
use moka::future::Cache as MokaCache;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, fmt::Debug, num::NonZeroUsize, sync::Arc, time::Duration};
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

    /// [`TransportService::pre_aggregation_matches`], remembered for the compiled model:
    /// whether a rollup serves a cube query changes only with the model, which is a new
    /// entry, or with the security context, which keys the answer along with the query
    async fn pre_aggregation_matches(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
        span_id: Option<Arc<SpanId>>,
        queries: Vec<TransportLoadRequestQuery>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
    ) -> Result<Vec<bool>, CubeError>;
}

// Rollup verdicts are small, so a model keeps many of them
const PRE_AGGREGATION_MATCHES_CACHE_SIZE: u64 = 10_000;

#[derive(Debug)]
pub struct CompilerCacheImpl {
    config_obj: Arc<dyn ConfigObj>,
    transport: Arc<dyn TransportService>,
    compiler_id_to_entry: MutexAsync<LruCache<(Uuid, DatabaseProtocol), Arc<CompilerCacheEntry>>>,
}

pub struct CompilerCacheEntry {
    meta_context: Arc<MetaContext>,
    rewrite_rules: RWLockAsync<HashMap<bool, Arc<Vec<CubeRewrite>>>>,
    parameterized_cache: MokaCache<[u8; 32], CubeEGraph>,
    queries_cache: MokaCache<[u8; 32], CubeEGraph>,
    pre_aggregation_matches: MokaCache<[u8; 32], bool>,
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

        if let Some(rewrite_entry) = cache_entry.parameterized_cache.get(&graph_key).await {
            return Ok(rewrite_entry);
        }

        let cache_entry_clone = Arc::clone(&cache_entry);
        let mut rewriter = Rewriter::new(parameterized_graph, cube_context);
        let rewrite_entry = rewriter
            .run_rewrite_to_completion(cache_entry_clone, qtrace)
            .await?;
        cache_entry
            .parameterized_cache
            .insert(graph_key, rewrite_entry.clone())
            .await;
        Ok(rewrite_entry)
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

        if let Some(plan) = cache_entry.queries_cache.get(&graph_key).await {
            return Ok(plan);
        }

        let cache_entry_clone = Arc::clone(&cache_entry);
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

        cache_entry
            .queries_cache
            .insert(graph_key, final_plan.clone())
            .await;

        Ok(final_plan)
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

        // Double-checked locking
        let cache_entry = if let Some(cache_entry) = cache_entry {
            cache_entry
        } else {
            let meta_context = self.transport.meta(ctx.clone()).await?;
            let mut compiler_id_to_entry = self.compiler_id_to_entry.lock().await;
            compiler_id_to_entry
                .get(&(meta_context.compiler_id, protocol.clone()))
                .cloned()
                .unwrap_or_else(|| {
                    let ttl = Duration::from_secs(self.config_obj.query_cache_time_to_idle_secs());
                    let cache_entry = Arc::new(CompilerCacheEntry {
                        meta_context: meta_context.clone(),
                        rewrite_rules: RWLockAsync::new(HashMap::new()),
                        parameterized_cache: MokaCache::builder()
                            .max_capacity(self.config_obj.query_cache_size())
                            .time_to_idle(ttl)
                            .build(),
                        queries_cache: MokaCache::builder()
                            .max_capacity(self.config_obj.query_cache_size())
                            .time_to_idle(ttl)
                            .build(),
                        // A verdict can go stale (`queryRewrite`), so it is not renewed
                        // on every read like the plans
                        pre_aggregation_matches: MokaCache::builder()
                            .max_capacity(PRE_AGGREGATION_MATCHES_CACHE_SIZE)
                            .time_to_live(ttl)
                            .build(),
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

    async fn pre_aggregation_matches(
        &self,
        cache_entry: Arc<CompilerCacheEntry>,
        span_id: Option<Arc<SpanId>>,
        queries: Vec<TransportLoadRequestQuery>,
        ctx: AuthContextRef,
        meta_fields: LoadRequestMeta,
    ) -> Result<Vec<bool>, CubeError> {
        // Hashed, so the entries stay small whatever the security context holds. The
        // request meta is in: `queryRewrite` sees it too
        let security_context = ctx
            .security_context()
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_default();
        let meta = serde_json::to_string(&meta_fields)?;
        let mut keys: Vec<[u8; 32]> = Vec::with_capacity(queries.len());
        for query in &queries {
            let mut hasher = Sha256::new();
            for part in [&security_context, &meta, &serde_json::to_string(query)?] {
                hasher.update(part.as_bytes());
                hasher.update(b"\0");
            }
            keys.push(hasher.finalize().into());
        }

        let mut matches = Vec::with_capacity(queries.len());
        let mut unknown: Vec<(usize, [u8; 32])> = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            match cache_entry.pre_aggregation_matches.get(key).await {
                Some(matched) => matches.push(Some(matched)),
                None => {
                    matches.push(None);
                    if !unknown.iter().any(|(_, known)| known == key) {
                        unknown.push((index, *key));
                    }
                }
            }
        }
        if !unknown.is_empty() {
            let answers = self
                .transport
                .pre_aggregation_matches(
                    span_id,
                    unknown
                        .iter()
                        .map(|(index, _)| queries[*index].clone())
                        .collect(),
                    ctx,
                    meta_fields,
                )
                .await?;
            if answers.len() != unknown.len() {
                return Err(CubeError::internal(format!(
                    "Pre-aggregation match answered for {} queries, asked for {}",
                    answers.len(),
                    unknown.len()
                )));
            }
            // The answers fill the verdicts themselves: the cache may not keep an entry
            // it was just given
            let answers: HashMap<[u8; 32], bool> =
                unknown.iter().map(|(_, key)| *key).zip(answers).collect();
            for (key, matched) in &answers {
                cache_entry
                    .pre_aggregation_matches
                    .insert(*key, *matched)
                    .await;
            }
            for (index, key) in keys.iter().enumerate() {
                if matches[index].is_none() {
                    matches[index] = answers.get(key).copied();
                }
            }
        }
        matches
            .into_iter()
            .map(|matched| {
                matched.ok_or_else(|| {
                    CubeError::internal("Pre-aggregation match verdict missing".to_string())
                })
            })
            .collect()
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
