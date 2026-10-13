use crate::{
    compile::{
        engine::df::scan::CubeScanNode,
        qtrace::{Qtrace, QtraceEclass, QtraceEgraphIteration},
        rewrite::{
            analysis::LogicalPlanAnalysis,
            converter::LanguageToLogicalPlanConverter,
            cost::{BestCubePlan, CubePlanTopDownState, TopDownExtractor},
            rules::{
                case::CaseRules, common::CommonRules, dates::DateRules, filters::FilterRules,
                flatten::FlattenRules, members::MemberRules, old_split::OldSplitRules,
                order::OrderRules, split::SplitRules, wrapper::WrapperRules,
            },
            CubeScanWrapped, LiteralExprValue, LogicalPlanLanguage, QueryParamIndex, ScanWrapping,
        },
        CubeContext,
    },
    config::ConfigObj,
    sql::database_variables::postgres::session_vars::CUBESQL_PENALIZE_POST_PROCESSING_VAR,
    sql::{compiler_cache::CompilerCacheEntry, AuthContextRef},
    transport::{MetaContext, SpanId, TransportLoadRequestQuery, CUBE_STORE_DATA_SOURCE},
    CubeError,
};
use datafusion::{
    logical_plan::{plan::Extension, LogicalPlan},
    physical_plan::planner::DefaultPhysicalPlanner,
    scalar::ScalarValue,
};
use egg::{EGraph, Id, IterationData, Language, RecExpr, Rewrite, Runner, StopReason};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    sync::Arc,
    time::{Duration, Instant},
};

pub type CubeRewrite = Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>;

/// How many plans are extracted at most while Cube keeps refusing pre-aggregations for
/// scans of the best one; the next extraction then reads none from a pre-aggregation
const MAX_PRE_AGGREGATION_EXTRACTIONS: usize = 10;
pub type CubeEGraph = EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>;

pub struct Rewriter {
    graph: CubeEGraph,
    cube_context: Arc<CubeContext>,
}

pub type CubeRunner = Runner<LogicalPlanLanguage, LogicalPlanAnalysis, IterInfo>;

#[derive(Clone, Serialize, Deserialize)]
struct DebugENodeId(String);

impl From<&LogicalPlanLanguage> for DebugENodeId {
    fn from(value: &LogicalPlanLanguage) -> Self {
        Self(format!("{value:?}"))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EClassDebugData {
    id: Id,
    canon: Id,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ENodeDebugData {
    enode: DebugENodeId,
    eclass: Id,
    children: Vec<Id>,
}

/// Representation is optimised for storing in JSON, to transfer to UI
#[derive(Clone, Serialize, Deserialize)]
pub struct EGraphDebugState {
    eclasses: Vec<EClassDebugData>,
    enodes: Vec<ENodeDebugData>,
}

impl EGraphDebugState {
    pub fn new(graph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>) -> Self {
        let current_eclasses = graph.classes().map(|ec| ec.id);
        let previous_debug_eclasses = graph
            .analysis
            .debug_states
            .iter()
            .flat_map(|state| state.eclasses.iter().map(|ecd| ecd.id));
        let all_known_eclasses = current_eclasses.chain(previous_debug_eclasses);

        let all_known_eclasses = all_known_eclasses.collect::<HashSet<_>>();

        let eclasses = all_known_eclasses
            .into_iter()
            .map(|ec| EClassDebugData {
                id: ec,
                canon: graph.find(ec),
            })
            .collect::<Vec<_>>();

        let enodes = graph
            .classes()
            .flat_map(|ec| ec.nodes.iter().map(move |node| (ec.id, node)))
            .map(|(ec, node)| ENodeDebugData {
                enode: node.into(),
                eclass: ec,
                children: node.children().to_vec(),
            })
            .collect();

        EGraphDebugState { eclasses, enodes }
    }
}

#[derive(Serialize, Deserialize)]
struct DebugState {
    egraph: EGraphDebugState,
    #[serde(rename = "appliedRules")]
    applied_rules: Vec<String>,
}

#[derive(Debug)]
pub struct IterInfo {
    debug_qtrace_eclasses: Option<Vec<QtraceEclass>>,
}

impl IterInfo {
    pub fn egraph_debug_enabled() -> bool {
        env::var("CUBESQL_DEBUG_EGRAPH")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false)
    }
}

impl IterationData<LogicalPlanLanguage, LogicalPlanAnalysis> for IterInfo {
    fn make(runner: &CubeRunner) -> Self {
        IterInfo {
            debug_qtrace_eclasses: if Qtrace::is_enabled() {
                Some(
                    runner
                        .egraph
                        .classes()
                        .map(|eclass| QtraceEclass::make(eclass))
                        .collect(),
                )
            } else {
                None
            },
        }
    }
}

fn write_debug_states(runner: &CubeRunner, stage: &str) -> Result<(), CubeError> {
    let dir = format!("egraph-debug-{}", stage);
    let _ = fs::create_dir_all(dir.clone());
    let _ = fs::create_dir_all(format!("{}/public", dir));
    let _ = fs::create_dir_all(format!("{}/src", dir));
    fs::copy(
        "egraph-debug-template/public/index.html",
        format!("{}/public/index.html", dir),
    )?;
    fs::copy(
        "egraph-debug-template/package.json",
        format!("{}/package.json", dir),
    )?;
    fs::copy(
        "egraph-debug-template/tsconfig.json",
        format!("{}/tsconfig.json", dir),
    )?;
    fs::copy(
        "egraph-debug-template/src/index.tsx",
        format!("{}/src/index.tsx", dir),
    )?;

    let debug_data = runner.egraph.analysis.debug_states.as_slice();
    debug_assert_eq!(debug_data.len(), runner.iterations.len() + 1);

    // debug_data[0] is initial state
    // runner.iterations[0] is result of first iteration
    let states_data = debug_data
        .iter()
        .skip(1)
        .zip(runner.iterations.iter().map(|i| Some(&i.applied)));
    let debug_data = std::iter::once((&debug_data[0], None))
        .chain(states_data)
        .map(|(egraph, applied_rules)| DebugState {
            egraph: egraph.clone(),
            applied_rules: applied_rules
                .map(|applied| applied.iter().map(|s| format!("{:?}", s)).collect())
                .unwrap_or(vec![]),
        })
        .collect::<Vec<_>>();

    fs::write(
        format!("{}/src/states.json", dir),
        serde_json::to_string_pretty(&debug_data)?,
    )?;

    Ok(())
}

impl Rewriter {
    pub fn new(graph: CubeEGraph, cube_context: Arc<CubeContext>) -> Self {
        Self {
            graph,
            cube_context,
        }
    }

    pub fn rewrite_runner(cube_context: Arc<CubeContext>, egraph: CubeEGraph) -> CubeRunner {
        let runner = CubeRunner::new(LogicalPlanAnalysis::new(
            cube_context,
            Arc::new(DefaultPhysicalPlanner::default()),
        ))
        // TODO move config to injector
        .with_iter_limit(
            env::var("CUBESQL_REWRITE_MAX_ITERATIONS")
                .map(|v| v.parse::<usize>().unwrap())
                .unwrap_or(500),
        )
        .with_node_limit(
            env::var("CUBESQL_REWRITE_MAX_NODES")
                .map(|v| v.parse::<usize>().unwrap())
                .unwrap_or(10000),
        )
        .with_time_limit(Duration::from_secs(
            env::var("CUBESQL_REWRITE_TIMEOUT")
                .map(|v| v.parse::<u64>().unwrap())
                .unwrap_or(30),
        ))
        .with_scheduler(IncrementalScheduler::default());

        let runner = if IterInfo::egraph_debug_enabled() {
            // We want more access than Iterations gives us
            // Specifically, there's no way to store and access egraph state before first iteration
            // This hook is not really order-dependent with iteration timestamp bump
            // But just for clarity it should run before, so first captured state would be when iteration is zero, before first iteration started
            runner.with_hook(|runner| {
                LogicalPlanAnalysis::store_egraph_debug_state(&mut runner.egraph);
                Ok(())
            })
        } else {
            runner
        };

        let runner = runner
            .with_hook(|runner| {
                runner.egraph.analysis.iteration_timestamp = runner.iterations.len() + 1;
                Ok(())
            })
            .with_egraph(egraph);

        runner
    }

    pub async fn run_rewrite_to_completion(
        &mut self,
        cache_entry: Arc<CompilerCacheEntry>,
        qtrace: &mut Option<Qtrace>,
    ) -> Result<CubeEGraph, CubeError> {
        let cube_context = self.cube_context.clone();
        let egraph = self.graph.clone();
        if let Some(qtrace) = qtrace {
            qtrace.set_original_graph(&egraph);
        }

        let rules = cube_context
            .sessions
            .server
            .compiler_cache
            .rewrite_rules(cache_entry, false)
            .await?;

        let (plan, qtrace_egraph_iterations) = tokio::task::spawn_blocking(move || {
            let (runner, qtrace_egraph_iterations) =
                Self::run_rewrites(&cube_context, egraph, rules, "intermediate")?;

            Ok::<_, CubeError>((runner.egraph, qtrace_egraph_iterations))
        })
        .await??;

        if let Some(qtrace) = qtrace {
            qtrace.set_egraph_iterations(qtrace_egraph_iterations);
        }

        Ok(plan)
    }

    pub fn add_param_values(
        &mut self,
        param_values: &HashMap<usize, ScalarValue>,
    ) -> Result<(), CubeError> {
        let mut query_param_id_to_value = HashMap::new();
        for (param_index, value) in param_values {
            // TODO use lookups instead of iteration
            for class in self.graph.classes() {
                for node in &class.nodes {
                    if let LogicalPlanLanguage::QueryParamIndex(QueryParamIndex(found_index)) = node
                    {
                        if found_index == param_index {
                            let query_param_id = self
                                .graph
                                .lookup(LogicalPlanLanguage::QueryParam([class.id]))
                                .ok_or_else(|| {
                                    CubeError::rewrite(format!(
                                        "Can't find param query node with id {}",
                                        class.id
                                    ))
                                })?;
                            query_param_id_to_value.insert(query_param_id, value.clone());
                        }
                    }
                }
            }
        }

        for (query_param_id, value) in query_param_id_to_value {
            let expr_value =
                self.graph
                    .add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                        value.clone(),
                    )));
            let literal_id = self
                .graph
                .add(LogicalPlanLanguage::LiteralExpr([expr_value]));
            self.graph.union(query_param_id, literal_id);
        }
        self.graph.rebuild();

        Ok(())
    }

    pub async fn find_best_plan(
        &mut self,
        root: Id,
        cache_entry: Arc<CompilerCacheEntry>,
        auth_context: AuthContextRef,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
    ) -> Result<LogicalPlan, CubeError> {
        let cube_context = self.cube_context.clone();
        let egraph = self.graph.clone();
        if let Some(qtrace) = qtrace {
            qtrace.set_original_graph(&egraph);
        }

        let rules = cube_context
            .sessions
            .server
            .compiler_cache
            .rewrite_rules(cache_entry.clone(), true)
            .await?;

        let penalize_post_processing = self
            .cube_context
            .session_state
            .get_variable(CUBESQL_PENALIZE_POST_PROCESSING_VAR)
            .map(|v| v.value);
        let penalize_post_processing = match penalize_post_processing {
            Some(ScalarValue::Boolean(val)) => val.unwrap_or(false),
            _ => false,
        };

        // In stream mode an unlimited Cube query is streamed in full rather than capped, so
        // post processing on top of it reads every row and stays correct. Neither the
        // preference for pushing it down nor the failure applies there
        let config_obj = &self.cube_context.sessions.server.config_obj;
        let penalize_limitless_post_processing = !config_obj.stream_mode();
        let max_row_limit = config_obj.non_streaming_query_max_row_limit().max(0) as usize;
        let fail_on_limitless_post_processing =
            config_obj.fail_on_limitless_post_processing() && penalize_limitless_post_processing;
        // Scans over pre-aggregations are in the graph only when both hold, see
        // `wrapper-cube-scan-wrap`
        let pre_aggregations = config_obj.sql_push_down_pre_aggregations()
            && cube_context
                .meta
                .data_source_to_sql_generator
                .contains_key(CUBE_STORE_DATA_SOURCE);
        let compiler_cache = cube_context.sessions.server.compiler_cache.clone();
        let load_request_meta = cube_context.session_state.get_load_request_meta("sql");

        let (egraph, qtrace_egraph_iterations) = {
            let cube_context = cube_context.clone();
            tokio::task::spawn_blocking(move || {
                let (runner, qtrace_egraph_iterations) =
                    Self::run_rewrites(&cube_context, egraph, rules, "final")?;
                Ok::<_, CubeError>((Arc::new(runner.egraph), qtrace_egraph_iterations))
            })
            .await??
        };

        // The best plan may read pre-aggregations Cube matches none for, which only Cube
        // can tell, so it is asked about the scans of each plan, and the scans it refuses
        // are extracted around until the plan has none of them
        let mut rejected: HashSet<LogicalPlanLanguage> = HashSet::new();
        let mut extraction = 0;
        let (best_cost, best) = loop {
            extraction += 1;
            // Past the bound, settle for the plan over the data source
            let prefer_pre_aggregations =
                pre_aggregations && extraction <= MAX_PRE_AGGREGATION_EXTRACTIONS;
            let extraction_start = Instant::now();
            let (cost, best, egraph_nodes) = {
                let egraph = egraph.clone();
                let meta = cube_context.meta.clone();
                let rejected = rejected.clone();
                tokio::task::spawn_blocking(move || {
                    // TODO maybe check replacers and penalized_ast_size_outside_wrapper right after extraction?
                    let mut extractor = TopDownExtractor::new(
                        egraph.as_ref(),
                        BestCubePlan::new(
                            meta,
                            penalize_post_processing,
                            penalize_limitless_post_processing,
                            max_row_limit,
                            prefer_pre_aggregations,
                            rejected,
                        ),
                        CubePlanTopDownState::new(max_row_limit),
                    );
                    extractor
                        .find_best_with_egraph_nodes(root)
                        .ok_or_else(|| CubeError::rewrite("Unable to find best plan".to_string()))
                })
                .await??
            };
            log::debug!(
                "Extraction {} took {:?}",
                extraction,
                extraction_start.elapsed()
            );
            if !prefer_pre_aggregations {
                break (cost, best);
            }

            // Conversion recurses over the plan, so it runs on a thread of its own
            let scans = {
                let best = best.clone();
                let cube_context = cube_context.clone();
                let auth_context = auth_context.clone();
                let span_id = span_id.clone();
                tokio::task::spawn_blocking(move || {
                    Self::pre_aggregation_scans(&best, &cube_context, &auth_context, &span_id)
                })
                .await??
            };
            let verdicts = if scans.is_empty() {
                vec![]
            } else {
                compiler_cache
                    .pre_aggregation_matches(
                        cache_entry.clone(),
                        span_id.clone(),
                        scans.iter().map(|(_, request)| request.clone()).collect(),
                        auth_context.clone(),
                        load_request_meta.clone(),
                    )
                    .await?
            };
            let mut newly_rejected = 0;
            for ((index, _), matched) in scans.iter().zip(verdicts) {
                if matched {
                    continue;
                }
                // Other spellings of the scan send a query matching the same rollups, so
                // they are refused along with it instead of one per round
                for node in Self::pre_aggregation_scans_alike(&egraph, &egraph_nodes[*index]) {
                    if rejected.insert(node) {
                        newly_rejected += 1;
                    }
                }
            }
            if newly_rejected == 0 {
                break (cost, best);
            }
            log::debug!(
                "Cube matched no pre-aggregation for {} scans of the plan, extracting again",
                newly_rejected
            );
        };
        log::debug!("Best cost: {:#?}", best_cost);
        let truncates_post_processing = best_cost.truncates_post_processing();

        let qtrace_best_graph = if Qtrace::is_enabled() {
            best.as_ref().to_vec()
        } else {
            vec![]
        };
        log::debug!("Best: {}", best.pretty(120));
        let plan = {
            let cube_context = cube_context.clone();
            let span_id = span_id.clone();
            tokio::task::spawn_blocking(move || {
                let new_root = Id::from(best.as_ref().len() - 1);
                // TODO maybe pass penalize_post_processing here as well, to break with sane error
                let converter =
                    LanguageToLogicalPlanConverter::new(best, cube_context, auth_context, span_id);
                let plan = converter.to_logical_plan(new_root);
                // Freed here, off the runtime workers: the graph can be huge
                drop(egraph);
                plan
            })
            .await?
        };

        if let Some(qtrace) = qtrace {
            qtrace.set_egraph_iterations(qtrace_egraph_iterations);
            qtrace.set_best_graph(&qtrace_best_graph);
        }

        // Checked once the qtrace is recorded: the plan that would have run is exactly what
        // is worth looking at when this fires. No representation of this query pushes the
        // post processing down to the data source, so it would run over a Cube query
        // truncated to the maximum row limit
        if fail_on_limitless_post_processing && truncates_post_processing {
            return Err(CubeError::user(
                "Query requires post-processing of a Cube query with no LIMIT, or with one \
                above the maximum row limit, so it would be truncated to that limit and \
                produce incorrect results. Add a LIMIT at or below it, or rewrite the query \
                so that it can be pushed down to the data source. This check is enabled by \
                CUBESQL_FAIL_ON_LIMITLESS_POST_PROCESSING."
                    .to_string(),
            ));
        }

        plan
    }

    /// `scan` and every scan over a pre-aggregation in the egraph with the same members,
    /// filters, grouping and join hints: whether a rollup matches depends on nothing
    /// else in a scan
    fn pre_aggregation_scans_alike(
        egraph: &CubeEGraph,
        scan: &LogicalPlanLanguage,
    ) -> Vec<LogicalPlanLanguage> {
        let LogicalPlanLanguage::CubeScan(scan_params) = scan else {
            return vec![scan.clone()];
        };
        let alike = |params: &[Id]| {
            params[1] == scan_params[1]
                && params[2] == scan_params[2]
                && params[9] == scan_params[9]
                && params[10] == scan_params[10]
        };
        egraph
            .classes()
            .flat_map(|class| class.nodes.iter())
            .filter(|node| {
                let LogicalPlanLanguage::CubeScan(params) = node else {
                    return false;
                };
                alike(params)
                    && egraph[params[8]].nodes.iter().any(|wrapped| {
                        matches!(
                            wrapped,
                            LogicalPlanLanguage::CubeScanWrapped(CubeScanWrapped(
                                ScanWrapping::PreAggregation
                            ))
                        )
                    })
            })
            .cloned()
            .chain(std::iter::once(scan.clone()))
            .collect()
    }

    /// The scans of `best` planned over a pre-aggregation: the index of each in `best`,
    /// with the cube query it sends, the one Cube is asked to match a rollup for
    fn pre_aggregation_scans(
        best: &RecExpr<LogicalPlanLanguage>,
        cube_context: &Arc<CubeContext>,
        auth_context: &AuthContextRef,
        span_id: &Option<Arc<SpanId>>,
    ) -> Result<Vec<(usize, TransportLoadRequestQuery)>, CubeError> {
        let nodes = best.as_ref();
        let indexes = nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| {
                let LogicalPlanLanguage::CubeScan(params) = node else {
                    return None;
                };
                let wrapped = &nodes[usize::from(params[8])];
                matches!(
                    wrapped,
                    LogicalPlanLanguage::CubeScanWrapped(CubeScanWrapped(
                        ScanWrapping::PreAggregation
                    ))
                )
                .then_some(index)
            })
            .collect::<Vec<_>>();
        if indexes.is_empty() {
            return Ok(vec![]);
        }
        let converter = LanguageToLogicalPlanConverter::new(
            best.clone(),
            cube_context.clone(),
            auth_context.clone(),
            span_id.clone(),
        );
        indexes
            .into_iter()
            .map(|index| {
                let LogicalPlan::Extension(Extension { node }) =
                    converter.to_logical_plan(Id::from(index))?
                else {
                    return Err(CubeError::internal(
                        "A scan over a pre-aggregation did not convert to a CubeScan".to_string(),
                    ));
                };
                let scan = node
                    .as_any()
                    .downcast_ref::<CubeScanNode>()
                    .ok_or_else(|| {
                        CubeError::internal(
                            "A scan over a pre-aggregation did not convert to a CubeScan"
                                .to_string(),
                        )
                    })?;
                Ok((index, scan.request.clone()))
            })
            .collect()
    }

    fn run_rewrites(
        cube_context: &Arc<CubeContext>,
        egraph: CubeEGraph,
        rules: Arc<Vec<CubeRewrite>>,
        stage: &str,
    ) -> Result<(CubeRunner, Vec<QtraceEgraphIteration>), CubeError> {
        let runner = Self::rewrite_runner(cube_context.clone(), egraph);
        let mut runner = runner.run(rules.iter());
        if !IterInfo::egraph_debug_enabled() {
            log::debug!("Iterations: {:?}", runner.iterations);
        }
        let stop_reason = &runner.iterations[runner.iterations.len() - 1].stop_reason;
        let stop_reason = match stop_reason {
            None => Some("timeout reached".to_string()),
            Some(StopReason::Saturated) => None,
            Some(StopReason::NodeLimit(limit)) => Some(format!("{} AST node limit reached", limit)),
            Some(StopReason::IterationLimit(limit)) => {
                Some(format!("{} iteration limit reached", limit))
            }
            Some(StopReason::Other(other)) => Some(other.to_string()),
            Some(StopReason::TimeLimit(seconds)) => {
                Some(format!("{} seconds timeout reached", seconds))
            }
        };
        if IterInfo::egraph_debug_enabled() {
            // Store final state after all rewrites
            LogicalPlanAnalysis::store_egraph_debug_state(&mut runner.egraph);
            write_debug_states(&runner, stage)?;
        }
        if let Some(stop_reason) = stop_reason {
            return Err(CubeError::rewrite(format!(
                "Can't find rewrite due to {}",
                stop_reason
            )));
        }
        let qtrace_egraph_iterations = if Qtrace::is_enabled() {
            runner
                .iterations
                .iter()
                .map(|iteration| {
                    QtraceEgraphIteration::make(
                        iteration,
                        iteration
                            .data
                            .debug_qtrace_eclasses
                            .as_ref()
                            .cloned()
                            .unwrap(),
                    )
                })
                .collect()
        } else {
            vec![]
        };
        Ok((runner, qtrace_egraph_iterations))
    }

    pub fn sql_push_down_enabled() -> bool {
        env::var("CUBESQL_SQL_PUSH_DOWN")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(true)
    }

    pub fn rewrite_rules(
        meta_context: Arc<MetaContext>,
        config_obj: Arc<dyn ConfigObj>,
        eval_stable_functions: bool,
    ) -> Vec<CubeRewrite> {
        let sql_push_down = Self::sql_push_down_enabled();
        let rules: &[&dyn RewriteRules] = &[
            &MemberRules::new(meta_context.clone(), config_obj.clone(), sql_push_down),
            &FilterRules::new(
                meta_context.clone(),
                config_obj.clone(),
                eval_stable_functions,
            ),
            &DateRules::new(config_obj.clone()),
            &OrderRules::new(config_obj.clone()),
            &CommonRules::new(config_obj.clone()),
        ];
        let mut rewrites = Vec::new();
        for r in rules {
            rewrites.extend(r.rewrite_rules());
        }
        if sql_push_down {
            rewrites.extend(
                WrapperRules::new(meta_context.clone(), config_obj.clone()).rewrite_rules(),
            );
            rewrites.extend(FlattenRules::new(config_obj.clone()).rewrite_rules());
        }
        if config_obj.push_down_pull_up_split() {
            rewrites
                .extend(SplitRules::new(meta_context.clone(), config_obj.clone()).rewrite_rules());
        } else {
            rewrites.extend(
                OldSplitRules::new(meta_context.clone(), config_obj.clone()).rewrite_rules(),
            );
            rewrites.extend(CaseRules::new().rewrite_rules());
        }
        if let Ok(disabled_rule_names) = env::var("CUBESQL_DISABLE_REWRITES") {
            let disabled_rule_names = disabled_rule_names
                .split(",")
                .map(|name| name.trim())
                .collect::<HashSet<_>>();
            let filtered_rewrites = rewrites
                .into_iter()
                .filter(|rewrite| !disabled_rule_names.contains(rewrite.name.as_str()))
                .collect();
            return filtered_rewrites;
        }
        rewrites
    }
}

pub trait RewriteRules {
    fn rewrite_rules(&self) -> Vec<CubeRewrite>;
}

struct IncrementalScheduler {
    current_iter: usize,
    current_eclasses: Vec<Id>,
}

impl Default for IncrementalScheduler {
    fn default() -> Self {
        Self {
            current_iter: usize::MAX, // force an update on the first iteration
            current_eclasses: Default::default(),
        }
    }
}

impl egg::RewriteScheduler<LogicalPlanLanguage, LogicalPlanAnalysis> for IncrementalScheduler {
    fn search_rewrite<'a>(
        &mut self,
        iteration: usize,
        egraph: &CubeEGraph,
        rewrite: &'a Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>,
    ) -> Vec<egg::SearchMatches<'a, LogicalPlanLanguage>> {
        if iteration != self.current_iter {
            self.current_iter = iteration;
            self.current_eclasses.clear();
            self.current_eclasses.extend(
                egraph
                    .classes()
                    .filter(|class| class.data.iteration_timestamp >= iteration)
                    .map(|class| class.id),
            );
        };
        assert_eq!(iteration, self.current_iter);
        rewrite.searcher.search_eclasses_with_limit(
            egraph,
            &mut self.current_eclasses.iter().copied(),
            usize::MAX,
        )
    }
}
