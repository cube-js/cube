use std::{env, fs, sync::Arc};

use cubeclient::models::V1LoadRequestQuery;
use datafusion::logical_plan::LogicalPlan;
use egg::{EClass, EGraph, Iteration, Language};
use serde::Serialize;
use sqlparser::ast::Statement;
use uuid::Uuid;

use super::{
    find_cube_scans_deep_search,
    rewrite::{
        analysis::{LogicalPlanAnalysis, LogicalPlanData},
        rewriter::IterInfo,
        LogicalPlanLanguage,
    },
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Qtrace {
    #[serde(rename = "cubesqlQtraceVersion")]
    version: (u64, u64),
    uuid: Uuid,
    original_query: String,
    replaced_query: Option<String>,
    statements: Vec<QtraceStatement>,
    error_message: Option<String>,
}

impl Qtrace {
    // Version of the qtrace schema, (major, minor).
    // The major component should be bumped whenever backwards incompatible changes are introduced.
    fn version() -> (u64, u64) {
        (1, 0)
    }

    pub fn new(original_query: &str) -> Option<Self> {
        if !Self::is_enabled() {
            return None;
        }
        Some(Self {
            version: Self::version(),
            uuid: Uuid::new_v4(),
            original_query: original_query.to_string(),
            replaced_query: None,
            statements: vec![],
            error_message: None,
        })
    }

    pub fn is_enabled() -> bool {
        env::var("CUBESQL_DEBUG_QTRACE")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false)
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn set_replaced_query(&mut self, query: &str) {
        self.replaced_query = Some(query.to_string());
    }

    pub fn push_statement(&mut self, statement: &Statement) {
        self.statements.push(QtraceStatement::new(statement));
    }

    pub fn statement(&mut self, fun: impl FnOnce(&mut QtraceStatement)) {
        if let Some(statement) = self.statements.last_mut() {
            fun(statement);
        }
    }

    pub fn set_visitor_replaced_statement(&mut self, statement: &Statement) {
        self.statement(|stmt| stmt.set_visitor_replaced_statement(statement));
    }

    pub fn set_df_plan(&mut self, plan: &LogicalPlan) {
        self.statement(|stmt| stmt.set_df_plan(plan));
    }

    pub fn set_optimized_plan(&mut self, plan: &LogicalPlan) {
        self.statement(|stmt| stmt.set_optimized_plan(plan));
    }

    pub fn set_original_graph(
        &mut self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    ) {
        self.statement(|stmt| stmt.set_original_graph(egraph));
    }

    pub fn set_egraph_iterations(&mut self, iterations: Vec<QtraceEgraphIteration>) {
        self.statement(|stmt| stmt.set_egraph_iterations(iterations));
    }

    pub fn set_best_graph(&mut self, nodes: &Vec<LogicalPlanLanguage>) {
        self.statement(|stmt| stmt.set_best_graph(nodes));
    }

    pub fn set_best_plan_and_cube_scans(&mut self, plan: &LogicalPlan) {
        self.statement(|stmt| stmt.set_best_plan_and_cube_scans(plan));
    }

    pub fn set_statement_error_message(&mut self, error_message: &str) {
        self.statement(|stmt| stmt.set_error_message(error_message));
    }

    pub fn set_query_error_message(&mut self, error_message: &str) {
        self.error_message = Some(error_message.to_string());
    }

    pub fn save_json(&self) {
        let debug_dir_name = Self::debug_dir_name();
        match fs::metadata(debug_dir_name) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    log::error!("Unable to create directory `{}`: there is already a file with the same name!", debug_dir_name);
                    return;
                }
            }
            Err(_) => {
                if let Err(error) = fs::create_dir(debug_dir_name) {
                    log::error!("Unable to create directory `{}`: {}", debug_dir_name, error);
                    return;
                }
            }
        };

        match serde_json::to_string_pretty(self) {
            Ok(json_string) => {
                let json_path = format!("{}/{}.json", debug_dir_name, self.uuid);
                if let Err(error) = std::fs::write(json_path, json_string) {
                    log::error!("Unable to write qtrace json to file: {}", error);
                    return;
                }
            }
            Err(error) => {
                log::error!("Unable to serialize qtrace to json: {}", error);
                return;
            }
        };
    }

    fn debug_dir_name() -> &'static str {
        "debug-qtrace"
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QtraceStatement {
    parsed_statement: String,
    visitor_replaced_statement: Option<String>,
    df_plan: Option<String>,
    optimized_plan: Option<String>,
    original_graph: Vec<QtraceEclass>,
    egraph_iterations: Vec<QtraceEgraphIteration>,
    best_graph: Vec<QtraceEclass>,
    best_plan: Option<String>,
    cube_scans: Option<Vec<V1LoadRequestQuery>>,
    error_message: Option<String>,
}

impl QtraceStatement {
    pub fn new(statement: &Statement) -> Self {
        Self {
            parsed_statement: statement.to_string(),
            visitor_replaced_statement: None,
            df_plan: None,
            optimized_plan: None,
            original_graph: vec![],
            egraph_iterations: vec![],
            best_graph: vec![],
            best_plan: None,
            cube_scans: None,
            error_message: None,
        }
    }

    pub fn set_visitor_replaced_statement(&mut self, statement: &Statement) {
        self.visitor_replaced_statement = Some(statement.to_string());
    }

    pub fn set_df_plan(&mut self, plan: &LogicalPlan) {
        self.df_plan = Some(format!("{:?}", plan));
    }

    pub fn set_optimized_plan(&mut self, plan: &LogicalPlan) {
        self.optimized_plan = Some(format!("{:?}", plan));
    }

    pub fn set_original_graph(
        &mut self,
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
    ) {
        self.original_graph = egraph
            .classes()
            .map(|eclass| QtraceEclass::make(eclass))
            .collect();
    }

    pub fn set_egraph_iterations(&mut self, iterations: Vec<QtraceEgraphIteration>) {
        self.egraph_iterations = iterations;
    }

    pub fn set_best_graph(&mut self, nodes: &Vec<LogicalPlanLanguage>) {
        self.best_graph = nodes
            .iter()
            .enumerate()
            .map(|(id, node)| QtraceEclass::new(id, vec![QtraceEnode::make(node)]))
            .collect();
    }

    pub fn set_best_plan_and_cube_scans(&mut self, plan: &LogicalPlan) {
        self.best_plan = Some(format!("{:?}", plan));
        self.cube_scans = Some(
            find_cube_scans_deep_search(Arc::new(plan.clone()), false)
                .into_iter()
                .map(|node| node.request)
                .collect(),
        );
    }

    pub fn set_error_message(&mut self, error_message: &str) {
        self.error_message = Some(error_message.to_string());
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QtraceEgraphIteration {
    eclasses: Vec<QtraceEclass>,
    applied_rules: Vec<QtraceAppliedRule>,
    hook_time: f64,
    search_time: f64,
    apply_time: f64,
    rebuild_time: f64,
}

impl QtraceEgraphIteration {
    pub fn make(iteration: &Iteration<IterInfo>, eclasses: Vec<QtraceEclass>) -> Self {
        Self {
            eclasses,
            applied_rules: iteration
                .applied
                .iter()
                .map(|(k, v)| QtraceAppliedRule::new(k.as_str(), *v))
                .collect(),
            hook_time: iteration.hook_time,
            search_time: iteration.search_time,
            apply_time: iteration.apply_time,
            rebuild_time: iteration.rebuild_time,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QtraceEclass {
    id: usize,
    nodes: Vec<QtraceEnode>,
}

impl QtraceEclass {
    pub fn new(id: usize, nodes: Vec<QtraceEnode>) -> Self {
        Self { id, nodes }
    }

    pub fn make(eclass: &EClass<LogicalPlanLanguage, LogicalPlanData>) -> Self {
        Self {
            id: usize::from(eclass.id),
            nodes: eclass
                .nodes
                .iter()
                .map(|node| QtraceEnode::make(node))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QtraceEnode {
    data: String,
    relations: Vec<usize>,
}

impl QtraceEnode {
    pub fn make(node: &LogicalPlanLanguage) -> Self {
        Self {
            data: format!("{:?}", node),
            relations: node.children().iter().map(|id| usize::from(*id)).collect(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QtraceAppliedRule {
    name: String,
    count: usize,
}

impl QtraceAppliedRule {
    pub fn new(name: &str, count: usize) -> Self {
        Self {
            name: name.to_string(),
            count,
        }
    }
}
