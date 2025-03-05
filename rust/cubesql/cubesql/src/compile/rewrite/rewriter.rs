use crate::{
    compile::{
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
            LiteralExprValue, LogicalPlanLanguage, QueryParamIndex,
        },
        CubeContext,
    },
    config::ConfigObj,
    sql::{compiler_cache::CompilerCacheEntry, AuthContextRef},
    transport::{MetaContext, SpanId},
    CubeError,
};
use datafusion::{
    logical_plan::LogicalPlan, physical_plan::planner::DefaultPhysicalPlanner, scalar::ScalarValue,
};
use egg::{EGraph, Extractor, Id, IterationData, Language, Rewrite, Runner, StopReason};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    sync::Arc,
    time::Duration,
};

pub type CubeRewrite = Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>;
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
                .unwrap_or(300),
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
                                    CubeError::internal(format!(
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
        top_down_extractor: bool,
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
            .rewrite_rules(cache_entry, true)
            .await?;

        let (plan, qtrace_egraph_iterations, qtrace_best_graph) =
            tokio::task::spawn_blocking(move || {
                let (runner, qtrace_egraph_iterations) =
                    Self::run_rewrites(&cube_context, egraph, rules, "final")?;

                let best = if top_down_extractor {
                    let mut extractor = TopDownExtractor::new(
                        &runner.egraph,
                        BestCubePlan::new(cube_context.meta.clone()),
                        CubePlanTopDownState::new(),
                    );
                    let Some((best_cost, best)) = extractor.find_best(root) else {
                        return Err(CubeError::internal("Unable to find best plan".to_string()));
                    };
                    log::debug!("Best cost: {:#?}", best_cost);
                    best
                } else {
                    let extractor = Extractor::new(
                        &runner.egraph,
                        BestCubePlan::new(cube_context.meta.clone()),
                    );
                    let (best_cost, best) = extractor.find_best(root);
                    log::debug!("Best cost: {:#?}", best_cost);
                    best
                };
                let qtrace_best_graph = if Qtrace::is_enabled() {
                    best.as_ref().to_vec()
                } else {
                    vec![]
                };
                let new_root = Id::from(best.as_ref().len() - 1);
                log::debug!("Best: {}", best.pretty(120));
                let converter = LanguageToLogicalPlanConverter::new(
                    best,
                    cube_context.clone(),
                    auth_context,
                    span_id.clone(),
                );
                Ok((
                    converter.to_logical_plan(new_root),
                    qtrace_egraph_iterations,
                    qtrace_best_graph,
                ))
            })
            .await??;

        if let Some(qtrace) = qtrace {
            qtrace.set_egraph_iterations(qtrace_egraph_iterations);
            qtrace.set_best_graph(&qtrace_best_graph);
        }

        plan
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
            return Err(CubeError::user(format!(
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

    pub fn top_down_extractor_enabled() -> bool {
        env::var("CUBESQL_TOP_DOWN_EXTRACTOR")
            .map(|v| v.to_lowercase() != "false")
            .unwrap_or(true)
    }

    pub fn rewrite_rules(
        meta_context: Arc<MetaContext>,
        config_obj: Arc<dyn ConfigObj>,
        eval_stable_functions: bool,
    ) -> Vec<CubeRewrite> {
        let sql_push_down = Self::sql_push_down_enabled();
        let rules: Vec<Box<dyn RewriteRules>> = vec![
            Box::new(MemberRules::new(
                meta_context.clone(),
                config_obj.clone(),
                sql_push_down,
            )),
            Box::new(FilterRules::new(
                meta_context.clone(),
                config_obj.clone(),
                eval_stable_functions,
            )),
            Box::new(DateRules::new(config_obj.clone())),
            Box::new(OrderRules::new()),
            Box::new(CommonRules::new(config_obj.clone())),
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
                    .filter(|class| (class.data.iteration_timestamp >= iteration))
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
