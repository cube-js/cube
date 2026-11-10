use super::*;
use crate::planner::{InputId, MemberSymbol};
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

    /// The plan without the inline subqueries nothing reads.
    pub fn without_unread_inline_subqueries(self: &Rc<Self>) -> Rc<Self> {
        let mut read = HashSet::new();
        collect_read_inputs(&self.query.as_plan_node(), &mut read);
        let mut kept_rev = Vec::new();
        for subquery in self.subqueries.iter().rev() {
            if subquery.inline && !read.contains(&subquery.id) {
                continue;
            }
            collect_read_inputs(&subquery.as_plan_node(), &mut read);
            kept_rev.push(subquery.clone());
        }
        if kept_rev.len() == self.subqueries.len() {
            return self.clone();
        }
        Rc::new(Self {
            subqueries: kept_rev.into_iter().rev().collect(),
            query: self.query.clone(),
        })
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

    /// Fails unless every node reading subqueries reads its members through
    /// references to them, except the readers that find their columns at render
    /// time. `is_masked` tells which members render their mask.
    pub fn check_input_refs(
        &self,
        is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
    ) -> Result<(), CubeError> {
        self.subqueries.iter().try_for_each(|subquery| {
            check_node_input_refs(&subquery.as_plan_node(), false, is_masked)
        })?;
        check_node_input_refs(&self.query.as_plan_node(), false, is_masked)
    }
}

// `read_by_parent`: the node is the source of a reader whose references cover
// the subqueries it links.
fn check_node_input_refs(
    node: &PlanNode,
    read_by_parent: bool,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
) -> Result<(), CubeError> {
    let reader = match node {
        PlanNode::Query(query) => Some(reader_check(query.as_ref(), is_masked)),
        PlanNode::MultiStageMeasureCalculation(calculation) => {
            Some(reader_check(calculation.as_ref(), is_masked))
        }
        PlanNode::MultiStageDimensionCalculation(calculation) => {
            Some(reader_check(calculation.as_ref(), is_masked))
        }
        _ => None,
    };
    let covers_children = match reader {
        Some(reads_inputs) => reads_inputs?,
        None => {
            if !read_by_parent && !reads_without_references(node) {
                if let Some(id) = node.referenced_inputs().first() {
                    return Err(CubeError::internal(format!(
                        "{} reads subquery {} without references to it",
                        node.node_name(),
                        id
                    )));
                }
            }
            // A query's source passes its reader on to the merge under it.
            read_by_parent && matches!(node, PlanNode::QuerySource(_))
        }
    };
    node.inputs()
        .iter()
        .try_for_each(|input| check_node_input_refs(input, covers_children, is_masked))
}

// Whether the reader reads subqueries, once its references to them check out.
fn reader_check(
    reader: &impl InputReader,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
) -> Result<bool, CubeError> {
    reader.check_input_refs(is_masked)?;
    Ok(!reader.read_inputs().is_empty())
}

// Readers whose columns of the subqueries they read are found at render time,
// not through references.
fn reads_without_references(node: &PlanNode) -> bool {
    matches!(
        node,
        PlanNode::QuerySource(_)
            | PlanNode::AggregateMultipliedSubquery(_)
            | PlanNode::MultiStageRollingWindow(_)
            | PlanNode::MultiStageTimeSeries(_)
            | PlanNode::KeysGrid(_)
    )
}

fn collect_read_inputs(node: &PlanNode, read: &mut HashSet<InputId>) {
    read.extend(node.referenced_inputs());
    for input in node.inputs() {
        collect_read_inputs(&input, read);
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
