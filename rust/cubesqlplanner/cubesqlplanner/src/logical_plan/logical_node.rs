use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait LogicalNode {
    fn inputs(&self) -> Vec<PlanNode>;

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError>;

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError>;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode;

    fn node_name(&self) -> &'static str;
}

#[derive(Clone)]
pub enum PlanNode {
    Query(Rc<Query>),
    LogicalJoin(Rc<LogicalJoin>),
    FullKeyAggregate(Rc<FullKeyAggregate>),
    PreAggregation(Rc<PreAggregation>),
    ResolveMultipliedMeasures(Rc<ResolveMultipliedMeasures>),
    AggregateMultipliedSubquery(Rc<AggregateMultipliedSubquery>),
    Cube(Rc<Cube>),
    MeasureSubquery(Rc<MeasureSubquery>),
    DimensionSubQuery(Rc<DimensionSubQuery>),
    KeysSubQuery(Rc<KeysSubQuery>),
    MultiStageGetDateRange(Rc<MultiStageGetDateRange>),
    MultiStageLeafMeasure(Rc<MultiStageLeafMeasure>),
    MultiStageMeasureCalculation(Rc<MultiStageMeasureCalculation>),
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
            PlanNode::ResolveMultipliedMeasures($node) => $block,
            PlanNode::AggregateMultipliedSubquery($node) => $block,
            PlanNode::Cube($node) => $block,
            PlanNode::MeasureSubquery($node) => $block,
            PlanNode::DimensionSubQuery($node) => $block,
            PlanNode::KeysSubQuery($node) => $block,
            PlanNode::MultiStageGetDateRange($node) => $block,
            PlanNode::MultiStageLeafMeasure($node) => $block,
            PlanNode::MultiStageMeasureCalculation($node) => $block,
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
