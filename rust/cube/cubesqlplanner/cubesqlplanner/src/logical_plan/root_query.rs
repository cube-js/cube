use super::*;
use crate::planner::InputId;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// The single root of a logical plan: the flat list of `subqueries` the
/// plan defines (in definition order — dependencies precede their
/// dependents) and the `query` body of the final SELECT. This is the
/// only node that carries subqueries, so a `WITH` clause can appear only
/// once, at the very top of the generated SQL.
#[derive(Clone, TypedBuilder)]
pub struct RootQuery {
    subqueries: Vec<Rc<Subquery>>,
    query: Rc<Query>,
}

impl RootQuery {
    pub fn subqueries(&self) -> &Vec<Rc<Subquery>> {
        &self.subqueries
    }
    pub fn query(&self) -> &Rc<Query> {
        &self.query
    }

    /// The plan reading `to` wherever it read the subquery `from`.
    pub fn forward(
        self: &Rc<Self>,
        from: InputId,
        to: &Rc<SubqueryRef>,
    ) -> Result<Rc<Self>, CubeError> {
        self.as_plan_node()
            .with_forwarded_input(from, to)?
            .into_logical_node()
    }

    /// Fails unless every subquery the plan reads is defined, once, before
    /// anything reading it.
    pub fn check_integrity(&self) -> Result<(), CubeError> {
        let mut defined = HashSet::new();
        for subquery in self.subqueries.iter() {
            check_inputs_defined(&subquery.as_plan_node(), &defined)?;
            if !defined.insert(subquery.id) {
                return Err(CubeError::internal(format!(
                    "Subquery {} is defined twice",
                    subquery.id
                )));
            }
        }
        check_inputs_defined(&self.query.as_plan_node(), &defined)
    }
}

fn check_inputs_defined(node: &PlanNode, defined: &HashSet<InputId>) -> Result<(), CubeError> {
    if let Some(id) = node
        .referenced_inputs()
        .into_iter()
        .find(|id| !defined.contains(id))
    {
        return Err(CubeError::internal(format!(
            "{} reads subquery {} that is not defined before it",
            node.node_name(),
            id
        )));
    }
    node.inputs()
        .iter()
        .try_for_each(|input| check_inputs_defined(input, defined))
}

impl LogicalNode for RootQuery {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::RootQuery(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        let mut result: Vec<PlanNode> = self
            .subqueries
            .iter()
            .map(|subquery| subquery.as_plan_node())
            .collect();
        result.push(self.query.as_plan_node());
        result
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, self.subqueries.len() + 1, self.node_name())?;
        let subqueries = inputs[0..self.subqueries.len()]
            .iter()
            .map(|subquery| subquery.clone().into_logical_node())
            .collect::<Result<Vec<_>, _>>()?;
        let query = inputs[self.subqueries.len()].clone().into_logical_node()?;

        Ok(Rc::new(Self { subqueries, query }))
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
        if !self.subqueries.is_empty() {
            result.println("subqueries:", &state);
            for subquery in self.subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
        result.println("query:", &state);
        self.query.pretty_print(result, &details_state);
    }
}
