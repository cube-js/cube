use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            analysis::LogicalPlanAnalysis,
            converter::LanguageToLogicalPlanConverter,
            cost::BestCubePlan,
            rules::{
                dates::DateRules, filters::FilterRules, members::MemberRules, order::OrderRules,
                split::SplitRules,
            },
            LogicalPlanLanguage,
        },
    },
    sql::AuthContextRef,
    CubeError,
};
use datafusion::{logical_plan::LogicalPlan, physical_plan::planner::DefaultPhysicalPlanner};
use egg::{EGraph, Extractor, Id, IterationData, Language, Rewrite, Runner, StopReason};
use itertools::Itertools;
use std::{env, ffi::OsStr, fs, io::Write, sync::Arc, time::Duration};

pub struct Rewriter {
    graph: EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    cube_context: Arc<CubeContext>,
}

type CubeRunner = Runner<LogicalPlanLanguage, LogicalPlanAnalysis, IterInfo>;

#[derive(Debug)]
pub struct IterDebugInfo {
    svg_file: String,
    formatted_egraph: String,
    formatted_nodes_csv: Vec<Vec<String>>,
    formatted_edges_csv: Vec<Vec<String>>,
}

impl IterDebugInfo {
    pub fn format_egraph(graph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>) -> String {
        let clusters = graph
            .classes()
            .map(|class| {
                let node_names = class
                    .nodes
                    .iter()
                    .map(|n| format!("{:?}", format!("{:?}", n)))
                    .collect::<Vec<_>>();
                let links = node_names
                    .iter()
                    .map(|n| {
                        format!(
                            "    {} [shape=rect];\n    {:?} -> {};\n",
                            n,
                            format!("#{}", class.id),
                            n
                        )
                    })
                    .join("\n");
                let external_links = class
                    .nodes
                    .iter()
                    .flat_map(|n| {
                        n.children().iter().map(move |c| {
                            format!("  {:?} -> {:?};", format!("{:?}", n), format!("#{}", c))
                        })
                    })
                    .collect::<Vec<_>>();
                (
                    format!(
                        "  subgraph cluster_{} {{\
\n    style=filled;\
\n    color=lightgrey;\
\n    node [style=filled,color=white];\
\n{}\
\n  }}",
                        class.id, links
                    ),
                    external_links,
                )
            })
            .collect::<Vec<_>>();
        format!(
            "digraph Egraph {{\
\n{}\
\n{}\
}}",
            clusters
                .iter()
                .map(|(cluster, _)| cluster.to_string())
                .join("\n"),
            clusters
                .iter()
                .map(|(_, links)| links.join("\n"))
                .join("\n"),
        )
    }

    pub fn format_nodes_csv(
        graph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        iteration_id: usize,
    ) -> Vec<Vec<String>> {
        let mut res = Vec::new();
        for class in graph.classes() {
            res.push(vec![
                class.id.to_string(),
                format!("#{}", class.id),
                class.id.to_string(),
                format!("<[{}.0, {}.0]>", iteration_id, iteration_id),
            ]);
            res.extend(
                class
                    .nodes
                    .iter()
                    .map(|n| {
                        vec![
                            format!("{:?}", n),
                            format!("{:?}", n),
                            class.id.to_string(),
                            format!("<[{}.0, {}.0]>", iteration_id, iteration_id),
                        ]
                    })
                    .collect::<Vec<_>>(),
            );
        }
        res
    }

    pub fn format_edges_csv(
        graph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        iteration_id: usize,
    ) -> Vec<Vec<String>> {
        let mut res = Vec::new();
        for class in graph.classes() {
            res.extend(
                class
                    .nodes
                    .iter()
                    .map(|n| {
                        vec![
                            class.id.to_string(),
                            format!("{:?}", n),
                            "directed".to_string(),
                            format!("<[{}.0, {}.0]>", iteration_id, iteration_id),
                        ]
                    })
                    .collect::<Vec<_>>(),
            );

            res.extend(
                class
                    .nodes
                    .iter()
                    .flat_map(|n| {
                        n.children().iter().map(move |c| {
                            vec![
                                format!("{:?}", n),
                                c.to_string(),
                                "directed".to_string(),
                                format!("<[{}.0, {}.0]>", iteration_id, iteration_id),
                            ]
                        })
                    })
                    .collect::<Vec<_>>(),
            );
        }
        res
    }

    pub fn run_dot<S, I>(graph: String, args: I) -> Result<(), CubeError>
    where
        S: AsRef<OsStr>,
        I: IntoIterator<Item = S>,
    {
        use std::process::{Command, Stdio};
        let mut child = Command::new(env::var("CUBESQL_DOT_PATH").unwrap_or("dot".to_string()))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?;
        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
        write!(stdin, "{}", graph)?;
        match child.wait()?.code() {
            Some(0) => Ok(()),
            Some(e) => Err(CubeError::internal(format!(
                "dot program returned error code {}",
                e
            ))),
            None => Err(CubeError::internal(
                "dot program was killed by a signal".to_string(),
            )),
        }
    }

    pub fn export_svg(&self) -> Result<(), CubeError> {
        Self::run_dot(
            self.formatted_egraph.to_string(),
            &["-Tsvg", "-o", self.svg_file.as_str()],
        )
    }

    fn make(runner: &CubeRunner) -> Self {
        let iteration_id = runner.iterations.len();
        let svg_file = format!("egraph-debug/iteration-{}.svg", iteration_id);
        let formatted_egraph = Self::format_egraph(&runner.egraph);
        IterDebugInfo {
            svg_file,
            formatted_egraph,
            formatted_nodes_csv: Self::format_nodes_csv(&runner.egraph, iteration_id),
            formatted_edges_csv: Self::format_edges_csv(&runner.egraph, iteration_id),
        }
    }
}

#[derive(Debug)]
pub struct IterInfo {
    debug_info: Option<IterDebugInfo>,
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
                .unwrap_or(15),
        ))
        .with_egraph(egraph)
    }

    pub async fn find_best_plan(
        &mut self,
        root: Id,
        auth_context: AuthContextRef,
    ) -> Result<LogicalPlan, CubeError> {
        let cube_context = self.cube_context.clone();
        let egraph = self.graph.clone();

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
            if let Some(stop_reason) = stop_reason {
                return Err(CubeError::user(format!(
                    "Can't find rewrite due to {}",
                    stop_reason
                )));
            }
            if IterInfo::egraph_debug_enabled() {
                let _ = fs::remove_dir_all("egraph-debug");
                let _ = fs::create_dir_all("egraph-debug");
                let mut nodes = csv::Writer::from_path("egraph-debug/nodes.csv")
                    .map_err(|e| CubeError::internal(e.to_string()))?;
                let mut edges = csv::Writer::from_path("egraph-debug/edges.csv")
                    .map_err(|e| CubeError::internal(e.to_string()))?;
                nodes
                    .write_record(&["Id", "Label", "Cluster", "Timeset"])
                    .map_err(|e| CubeError::internal(e.to_string()))?;
                edges
                    .write_record(&["Source", "Target", "Type", "Timeset"])
                    .map_err(|e| CubeError::internal(e.to_string()))?;
                for i in runner.iterations {
                    i.data.debug_info.as_ref().unwrap().export_svg()?;
                    for node in i
                        .data
                        .debug_info
                        .as_ref()
                        .unwrap()
                        .formatted_nodes_csv
                        .iter()
                    {
                        nodes
                            .write_record(node)
                            .map_err(|e| CubeError::internal(e.to_string()))?;
                    }
                    for edge in i
                        .data
                        .debug_info
                        .as_ref()
                        .unwrap()
                        .formatted_edges_csv
                        .iter()
                    {
                        edges
                            .write_record(edge)
                            .map_err(|e| CubeError::internal(e.to_string()))?;
                    }
                }
            }
            let extractor = Extractor::new(&runner.egraph, BestCubePlan);
            let (_, best) = extractor.find_best(root);
            let new_root = Id::from(best.as_ref().len() - 1);
            log::debug!("Best: {:?}", best);
            let converter =
                LanguageToLogicalPlanConverter::new(best, cube_context.clone(), auth_context);
            Ok(converter.to_logical_plan(new_root))
        })
        .await??
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
        ];
        let mut rewrites = Vec::new();
        for r in rules {
            rewrites.extend(r.rewrite_rules());
        }
        rewrites
    }
}

pub trait RewriteRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>;
}
