use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// The single root of a logical plan: the flat list of `ctes` the
/// plan defines (in definition order — dependencies precede their
/// dependents) and the `query` body of the final SELECT. This is the
/// only node that carries CTEs, so a `WITH` clause can appear only
/// once, at the very top of the generated SQL.
#[derive(Clone, TypedBuilder)]
pub struct RootQuery {
    ctes: Vec<Rc<LogicalMultiStageMember>>,
    query: Rc<Query>,
}

impl RootQuery {
    pub fn ctes(&self) -> &Vec<Rc<LogicalMultiStageMember>> {
        &self.ctes
    }
    pub fn query(&self) -> &Rc<Query> {
        &self.query
    }
}

impl LogicalNode for RootQuery {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::RootQuery(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        let mut result: Vec<PlanNode> = self.ctes.iter().map(|cte| cte.as_plan_node()).collect();
        result.push(self.query.as_plan_node());
        result
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, self.ctes.len() + 1, self.node_name())?;
        let ctes = inputs[0..self.ctes.len()]
            .iter()
            .map(|cte| cte.clone().into_logical_node())
            .collect::<Result<Vec<_>, _>>()?;
        let query = inputs[self.ctes.len()].clone().into_logical_node()?;

        Ok(Rc::new(Self { ctes, query }))
    }

    fn node_name(&self) -> &'static str {
        "RootQuery"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::RootQuery(root) = plan_node {
            Ok(root)
        } else {
            Err(cast_error(&plan_node, "RootQuery"))
        }
    }
}

impl PrettyPrint for RootQuery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("RootQuery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        if !self.ctes.is_empty() {
            result.println("ctes:", &state);
            for cte in self.ctes.iter() {
                cte.pretty_print(result, &details_state);
            }
        }
        result.println("query:", &state);
        self.query.pretty_print(result, &details_state);
    }
}
