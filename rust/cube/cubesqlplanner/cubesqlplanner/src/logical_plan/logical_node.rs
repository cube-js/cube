use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Node of the logical-plan tree. Exposes its child nodes through
/// `inputs()` / `with_inputs()` so generic passes can walk and
/// rewrite the tree without knowing concrete node types.
pub trait LogicalNode {
    fn inputs(&self) -> Vec<PlanNode>;

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError>;

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError>;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode;

    fn node_name(&self) -> &'static str;

    /// Names of subqueries this node references directly (not through its
    /// inputs). Any node that links to a subquery by name MUST override
    /// this — reachability passes (e.g. dropping orphaned subqueries in the
    /// pre-aggregation optimizer) rely on it.
    fn referenced_subqueries(&self) -> Vec<String> {
        vec![]
    }
}

/// Type-erased handle for a logical-plan node. Generic traversal
/// works in terms of `PlanNode`; concrete typed access is recovered
/// via `into_logical_node` / each node's `try_from_plan_node`.
#[derive(Clone)]
pub enum PlanNode {
    RootQuery(Rc<RootQuery>),
    Query(Rc<Query>),
    QuerySource(Rc<QuerySource>),
    QueryInputs(Rc<QueryInputs>),
    Merge(Rc<Merge>),
    PreAggregation(Rc<PreAggregation>),
    AggregateMultipliedSubquery(Rc<AggregateMultipliedSubquery>),
    Cube(Rc<Cube>),
    DimensionSubQuery(Rc<DimensionSubQuery>),
    MultiStageGetDateRange(Rc<MultiStageGetDateRange>),
    MultiStageMeasureCalculation(Rc<MultiStageMeasureCalculation>),
    MultiStageDimensionCalculation(Rc<MultiStageDimensionCalculation>),
    MultiStageTimeSeries(Rc<MultiStageTimeSeries>),
    MultiStageRollingWindow(Rc<MultiStageRollingWindow>),
    Subquery(Rc<Subquery>),
}

// Macro for applying a block to all enum variants
macro_rules! match_plan_node {
    ($self:expr, $node:ident => $block:block) => {
        match $self {
            PlanNode::RootQuery($node) => $block,
            PlanNode::Query($node) => $block,
            PlanNode::QuerySource($node) => $block,
            PlanNode::QueryInputs($node) => $block,
            PlanNode::Merge($node) => $block,
            PlanNode::PreAggregation($node) => $block,
            PlanNode::AggregateMultipliedSubquery($node) => $block,
            PlanNode::Cube($node) => $block,
            PlanNode::DimensionSubQuery($node) => $block,
            PlanNode::MultiStageGetDateRange($node) => $block,
            PlanNode::MultiStageMeasureCalculation($node) => $block,
            PlanNode::MultiStageDimensionCalculation($node) => $block,
            PlanNode::MultiStageTimeSeries($node) => $block,
            PlanNode::MultiStageRollingWindow($node) => $block,
            PlanNode::Subquery($node) => $block,
        }
    };
}

impl PlanNode {
    pub fn into_logical_node<T: LogicalNode>(self) -> Result<Rc<T>, CubeError> {
        T::try_from_plan_node(self)
    }

    pub fn inputs(&self) -> Vec<PlanNode> {
        match_plan_node!(self, node => {
            node.inputs()
        })
    }

    pub fn node_name(&self) -> &'static str {
        match_plan_node!(self, node => {
            node.node_name()
        })
    }

    pub fn referenced_subqueries(&self) -> Vec<String> {
        match_plan_node!(self, node => {
            node.referenced_subqueries()
        })
    }

    pub fn with_inputs(self, inputs: Vec<PlanNode>) -> Result<Self, CubeError> {
        let result = match_plan_node!(self, node => {
            node.with_inputs(inputs)?.as_plan_node()
        });
        Ok(result)
    }
}

pub(super) fn cast_error(plan_node: &PlanNode, target_type: &str) -> CubeError {
    CubeError::internal(format!(
        "Can't cast {} PlanNode into {}",
        plan_node.node_name(),
        target_type,
    ))
}

pub(super) fn check_inputs_len(
    inputs: &Vec<PlanNode>,
    expected: usize,
    node_type: &str,
) -> Result<(), CubeError> {
    if inputs.len() == expected {
        Ok(())
    } else {
        Err(CubeError::internal(format!(
            "{} received {} inputs, but {} were expected",
            node_type,
            inputs.len(),
            expected
        )))
    }
}
