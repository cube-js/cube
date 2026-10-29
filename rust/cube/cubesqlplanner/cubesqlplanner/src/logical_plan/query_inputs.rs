use super::pretty_print::*;
use super::*;
use crate::planner::{CubeId, JoinTree, SqlCall};
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// A query-level join against an opaque pre-rendered sub-query, sourced
/// from the SQL API `subqueryJoins`. `sql` is a complete SELECT emitted
/// verbatim; `on_sql` is the compiled join condition (it references the
/// owning cube and the sub-query `alias` as a literal).
#[derive(Clone)]
pub struct LogicalSubqueryJoinItem {
    pub sql: String,
    pub alias: String,
    pub join_type: String,
    pub on_sql: Rc<SqlCall>,
}

impl PrettyPrint for LogicalSubqueryJoinItem {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!("SubqueryJoinItem: {} AS {}", self.join_type, self.alias),
            state,
        );
    }
}

/// What a query reads from the model; how the inputs are combined in SQL is
/// up to the physical plan.
#[derive(Clone, TypedBuilder)]
pub struct QueryInputs {
    #[builder(default)]
    join: Option<Rc<JoinTree>>,
    #[builder(default)]
    subquery_joins: Vec<LogicalSubqueryJoinItem>,
    /// Prefix for the aliases of the cubes, so the same cubes can be read
    /// again by the select reading this one.
    #[builder(default)]
    alias_prefix: Option<String>,
}

impl QueryInputs {
    pub fn join(&self) -> &Option<Rc<JoinTree>> {
        &self.join
    }

    pub fn subquery_joins(&self) -> &Vec<LogicalSubqueryJoinItem> {
        &self.subquery_joins
    }

    /// Cubes the join reads, root first.
    pub fn cube_ids(&self) -> Vec<CubeId> {
        let Some(join) = &self.join else {
            return vec![];
        };
        std::iter::once(join.root().cube_id().clone())
            .chain(
                join.joins()
                    .iter()
                    .map(|item| item.cube().cube_id().clone()),
            )
            .collect()
    }

    pub fn alias_prefix(&self) -> &Option<String> {
        &self.alias_prefix
    }

    pub fn with_alias_prefix(&self, alias_prefix: String) -> Rc<Self> {
        Rc::new(Self {
            alias_prefix: Some(alias_prefix),
            ..self.clone()
        })
    }

    pub fn with_subquery_joins(&self, subquery_joins: Vec<LogicalSubqueryJoinItem>) -> Rc<Self> {
        Rc::new(Self {
            subquery_joins,
            ..self.clone()
        })
    }
}

impl LogicalNode for QueryInputs {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::QueryInputs(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
    }

    fn node_name(&self) -> &'static str {
        "QueryInputs"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::QueryInputs(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "QueryInputs"))
        }
    }
}

impl PrettyPrint for QueryInputs {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("QueryInputs:", state);
        let state = state.new_level();
        let details_state = state.new_level();
        if let Some(alias_prefix) = &self.alias_prefix {
            result.println(&format!("alias_prefix: {}", alias_prefix), &state);
        }
        if let Some(join) = &self.join {
            result.println(&format!("root: {}", join.root().cube_id()), &state);
            if !join.joins().is_empty() {
                result.println("joins:", &state);
                for item in join.joins().iter() {
                    result.println(&format!("{}", item.cube().cube_id()), &details_state);
                }
            }
        }
        if !self.subquery_joins.is_empty() {
            result.println("subquery_joins:", &state);
            for subquery_join in self.subquery_joins.iter() {
                subquery_join.pretty_print(result, &details_state);
            }
        }
    }
}
