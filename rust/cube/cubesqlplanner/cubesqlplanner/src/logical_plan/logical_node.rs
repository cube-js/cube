use super::pretty_print::*;
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
}

/// Type-erased handle for a logical-plan node. Generic traversal
/// works in terms of `PlanNode`; concrete typed access is recovered
/// via `into_logical_node` / each node's `try_from_plan_node`.
#[derive(Clone)]
pub enum PlanNode {
    Query(Rc<Query>),
    LogicalJoin(Rc<LogicalJoin>),
    FullKeyAggregate(Rc<FullKeyAggregate>),
    PreAggregation(Rc<PreAggregation>),
    Cube(Rc<Cube>),
    MultiStageTimeSeries(Rc<MultiStageTimeSeries>),
    MultiStageRollingWindow(Rc<MultiStageRollingWindow>),
    LogicalMultiStageMember(Rc<LogicalMultiStageMember>),
}

// Macro for applying a block to all enum variants
macro_rules! match_plan_node {
    ($self:expr, $node:ident => $block:block) => {
        match $self {
            PlanNode::Query($node) => $block,
            PlanNode::LogicalJoin($node) => $block,
            PlanNode::FullKeyAggregate($node) => $block,
            PlanNode::PreAggregation($node) => $block,
            PlanNode::Cube($node) => $block,
            PlanNode::MultiStageTimeSeries($node) => $block,
            PlanNode::MultiStageRollingWindow($node) => $block,
            PlanNode::LogicalMultiStageMember($node) => $block,
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

    pub fn with_inputs(self, inputs: Vec<PlanNode>) -> Result<Self, CubeError> {
        let result = match_plan_node!(self, node => {
            node.with_inputs(inputs)?.as_plan_node()
        });
        Ok(result)
    }

    /// Semantic classification — leaf/stage distinction independent of where
    /// the node currently sits in the plan structure.
    /// Returns `None` only for nodes that are pure plan scaffolding and do
    /// not produce a SELECT-shaped result on their own.
    pub fn multi_stage_kind(&self) -> Option<MultiStageKind> {
        match self {
            // Leaves — produce a CTE from base sources, no multi-stage CTE deps.
            // `Query` covers both true leaves and the aggregate-multiplied
            // subquery shape (`FullKeyAggregate` over already-published
            // KS/MS CTEs); the latter is conceptually a Stage but is
            // structurally a Query at this point.
            PlanNode::Query(_) | PlanNode::MultiStageTimeSeries(_) => Some(MultiStageKind::Leaf),

            PlanNode::MultiStageRollingWindow(_) => Some(MultiStageKind::Stage),

            // Pure plan scaffolding — never has a SELECT result on its own.
            PlanNode::LogicalJoin(_)
            | PlanNode::FullKeyAggregate(_)
            | PlanNode::PreAggregation(_)
            | PlanNode::Cube(_)
            | PlanNode::LogicalMultiStageMember(_) => None,
        }
    }
}

impl PrettyPrint for PlanNode {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match_plan_node!(self, node => {
            node.pretty_print(result, state);
        });
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
