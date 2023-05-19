use crate::{
    compile::{
        engine::provider::CubeContext,
        qtrace::{Qtrace, QtraceEclass, QtraceEgraphIteration},
        rewrite::{
            analysis::LogicalPlanAnalysis,
            converter::LanguageToLogicalPlanConverter,
            cost::BestCubePlan,
            rules::{
                case::CaseRules, dates::DateRules, filters::FilterRules, members::MemberRules,
                order::OrderRules, split::SplitRules, wrapper::WrapperRules,
            },
            LogicalPlanLanguage,
        },
    },
    sql::AuthContextRef,
    CubeError,
};
use datafusion::{logical_plan::LogicalPlan, physical_plan::planner::DefaultPhysicalPlanner};
use egg::{EGraph, Extractor, Id, IterationData, Language, Rewrite, Runner, StopReason};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, env, fs, sync::Arc, time::Duration};

pub struct Rewriter {
    graph: EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    cube_context: Arc<CubeContext>,
}

pub type CubeRunner = Runner<LogicalPlanLanguage, LogicalPlanAnalysis, IterInfo>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugNode {
    id: String,
    label: String,
    #[serde(rename = "comboId")]
    combo_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugEdge {
    source: String,
    target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugCombo {
    id: String,
    label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugData {
    nodes: Vec<DebugNode>,
    #[serde(rename = "removedNodes")]
    removed_nodes: Vec<DebugNode>,
    edges: Vec<DebugEdge>,
    #[serde(rename = "removedEdges")]
    removed_edges: Vec<DebugEdge>,
    combos: Vec<DebugCombo>,
    #[serde(rename = "removedCombos")]
    removed_combos: Vec<DebugCombo>,
    #[serde(rename = "appliedRules")]
    applied_rules: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct IterDebugInfo {
    debug_data: DebugData,
}

impl IterDebugInfo {
    pub fn prepare_debug_data(
        graph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    ) -> DebugData {
        DebugData {
            applied_rules: None,
            nodes: graph
                .classes()
                .flat_map(|class| {
                    let mut result = class
                        .nodes
                        .iter()
                        .map(|n| {
                            let node_id = format!("{}-{:?}", class.id, n);
                            DebugNode {
                                id: node_id.to_string(),
                                label: format!("{:?}", n),
                                combo_id: format!("c{}", class.id),
                            }
                        })
                        .collect::<Vec<_>>();
                    result.push(DebugNode {
                        id: class.id.to_string(),
                        label: class.id.to_string(),
                        combo_id: format!("c{}", class.id),
                    });
                    result
                })
                .collect(),
            edges: graph
                .classes()
                .flat_map(|class| {
                    class
                        .nodes
                        .iter()
                        .map(|n| DebugEdge {
                            source: class.id.to_string(),
                            target: format!("{}-{:?}", class.id, n,),
                        })
                        .chain(class.nodes.iter().flat_map(|n| {
                            n.children().iter().map(move |c| DebugEdge {
                                source: format!("{}-{:?}", class.id, n),
                                target: c.to_string(),
                            })
                        }))
                        .collect::<Vec<_>>()
                })
                .collect(),
            combos: graph
                .classes()
                .map(|class| DebugCombo {
                    id: format!("c{}", class.id),
                    label: format!("#{}", class.id),
                })
                .collect(),
            removed_nodes: Vec::new(),
            removed_edges: Vec::new(),
            removed_combos: Vec::new(),
        }
    }

    fn make(runner: &CubeRunner) -> Self {
        IterDebugInfo {
            debug_data: Self::prepare_debug_data(&runner.egraph),
        }
    }
}

#[derive(Debug)]
pub struct IterInfo {
    debug_info: Option<IterDebugInfo>,
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
            debug_info: if Self::egraph_debug_enabled() {
                Some(IterDebugInfo::make(runner))
            } else {
                None
            },
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

impl Rewriter {
    pub fn new(
        graph: EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        cube_context: Arc<CubeContext>,
    ) -> Self {
        Self {
            graph,
            cube_context,
        }
    }

    pub fn rewrite_runner(
        cube_context: Arc<CubeContext>,
        egraph: EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    ) -> CubeRunner {
        CubeRunner::new(LogicalPlanAnalysis::new(
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
        .with_egraph(egraph)
    }

    pub async fn find_best_plan(
        &mut self,
        root: Id,
        auth_context: AuthContextRef,
        qtrace: &mut Option<Qtrace>,
    ) -> Result<LogicalPlan, CubeError> {
        let cube_context = self.cube_context.clone();
        let egraph = self.graph.clone();
        if let Some(qtrace) = qtrace {
            qtrace.set_original_graph(&egraph);
        }

        let (plan, qtrace_egraph_iterations, qtrace_best_graph) =
            tokio::task::spawn_blocking(move || {
                let rules = Self::rewrite_rules(cube_context.clone());
                let runner = Self::rewrite_runner(cube_context.clone(), egraph);
                let runner = runner.run(rules.iter());
                if !IterInfo::egraph_debug_enabled() {
                    log::debug!("Iterations: {:?}", runner.iterations);
                }
                let stop_reason = &runner.iterations[runner.iterations.len() - 1].stop_reason;
                let stop_reason = match stop_reason {
                    None => Some("timeout reached".to_string()),
                    Some(StopReason::Saturated) => None,
                    Some(StopReason::NodeLimit(limit)) => {
                        Some(format!("{} AST node limit reached", limit))
                    }
                    Some(StopReason::IterationLimit(limit)) => {
                        Some(format!("{} iteration limit reached", limit))
                    }
                    Some(StopReason::Other(other)) => Some(other.to_string()),
                    Some(StopReason::TimeLimit(seconds)) => {
                        Some(format!("{} seconds timeout reached", seconds))
                    }
                };
                if IterInfo::egraph_debug_enabled() {
                    let _ = fs::create_dir_all("egraph-debug");
                    let _ = fs::create_dir_all("egraph-debug/public");
                    let _ = fs::create_dir_all("egraph-debug/src");
                    fs::copy(
                        "egraph-debug-template/public/index.html",
                        "egraph-debug/public/index.html",
                    )?;
                    fs::copy(
                        "egraph-debug-template/package.json",
                        "egraph-debug/package.json",
                    )?;
                    fs::copy(
                        "egraph-debug-template/src/index.js",
                        "egraph-debug/src/index.js",
                    )?;

                    let mut iterations = Vec::new();
                    let mut last_debug_data: Option<DebugData> = None;
                    for i in &runner.iterations {
                        let debug_data_clone =
                            i.data.debug_info.as_ref().unwrap().debug_data.clone();
                        let mut debug_data = i.data.debug_info.as_ref().unwrap().debug_data.clone();
                        if let Some(last) = last_debug_data {
                            debug_data
                                .nodes
                                .retain(|n| !last.nodes.iter().any(|ln| ln.id == n.id));
                            debug_data.edges.retain(|n| {
                                !last
                                    .edges
                                    .iter()
                                    .any(|ln| ln.source == n.source && ln.target == n.target)
                            });
                            debug_data
                                .combos
                                .retain(|n| !last.combos.iter().any(|ln| ln.id == n.id));

                            debug_data.removed_nodes = last.nodes.clone();
                            debug_data
                                .removed_nodes
                                .retain(|n| !debug_data_clone.nodes.iter().any(|ln| ln.id == n.id));
                            debug_data.removed_edges = last.edges.clone();
                            debug_data.removed_edges.retain(|n| {
                                !debug_data_clone
                                    .edges
                                    .iter()
                                    .any(|ln| ln.source == n.source && ln.target == n.target)
                            });
                            debug_data.removed_combos = last.combos.clone();
                            debug_data.removed_combos.retain(|n| {
                                !debug_data_clone.combos.iter().any(|ln| ln.id == n.id)
                            });
                        }
                        debug_data.applied_rules =
                            Some(i.applied.iter().map(|s| format!("{:?}", s)).collect());
                        iterations.push(debug_data);
                        last_debug_data = Some(debug_data_clone);
                    }
                    fs::write(
                        "egraph-debug/src/iterations.js",
                        &format!(
                            "export const iterations = {};",
                            serde_json::to_string_pretty(&iterations)?
                        ),
                    )?;
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
                let extractor = Extractor::new(&runner.egraph, BestCubePlan);
                let (_, best) = extractor.find_best(root);
                let qtrace_best_graph = if Qtrace::is_enabled() {
                    best.as_ref().iter().cloned().collect()
                } else {
                    vec![]
                };
                let new_root = Id::from(best.as_ref().len() - 1);
                log::debug!("Best: {:?}", best);
                let converter =
                    LanguageToLogicalPlanConverter::new(best, cube_context.clone(), auth_context);
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

    pub fn rewrite_rules(
        cube_context: Arc<CubeContext>,
    ) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        let rules: Vec<Box<dyn RewriteRules>> = vec![
            Box::new(MemberRules::new(cube_context.clone())),
            Box::new(FilterRules::new(cube_context.clone())),
            Box::new(DateRules::new(cube_context.clone())),
            Box::new(OrderRules::new(cube_context.clone())),
            Box::new(SplitRules::new(cube_context.clone())),
            Box::new(CaseRules::new(cube_context.clone())),
            Box::new(WrapperRules::new(cube_context.clone())),
        ];
        let mut rewrites = Vec::new();
        for r in rules {
            rewrites.extend(r.rewrite_rules());
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
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>;
}
