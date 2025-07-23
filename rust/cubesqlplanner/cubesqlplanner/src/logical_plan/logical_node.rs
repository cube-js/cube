use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait NodeInputs {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_>;
    fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut PlanNode> + '_>;
}

pub struct EmptyNodeInput {}
impl EmptyNodeInput {
    pub fn new() -> Self {
        Self {}
    }
}

impl NodeInputs for EmptyNodeInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(std::iter::empty())
    }

    fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut PlanNode> + '_> {
        Box::new(std::iter::empty())
    }
}

pub struct SingleNodeInput {
    item: PlanNode,
}

impl SingleNodeInput {
    pub fn new(item: PlanNode) -> Self {
        Self { item }
    }

    pub fn unpack(self) -> PlanNode {
        self.item
    }
}

impl NodeInputs for SingleNodeInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(std::iter::once(&self.item))
    }

    fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut PlanNode> + '_> {
        Box::new(std::iter::once(&mut self.item))
    }
}

pub struct OptionNodeInput {
    item: Option<PlanNode>,
}

impl OptionNodeInput {
    pub fn new(item: Option<PlanNode>) -> Self {
        Self { item }
    }

    pub fn unpack(self) -> Option<PlanNode> {
        self.item
    }
}

impl NodeInputs for OptionNodeInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        if let Some(item) = &self.item {
            Box::new(std::iter::once(item))
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut PlanNode> + '_> {
        if let Some(item) = &mut self.item {
            Box::new(std::iter::once(item))
        } else {
            Box::new(std::iter::empty())
        }
    }
}

pub struct VecNodeInput {
    items: Vec<PlanNode>,
}

impl VecNodeInput {
    pub fn new(items: Vec<PlanNode>) -> Self {
        Self { items }
    }

    pub fn unpack(self) -> Vec<PlanNode> {
        self.items
    }
}

impl NodeInputs for VecNodeInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(self.items.iter())
    }

    fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut PlanNode> + '_> {
        Box::new(self.items.iter_mut())
    }
}

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

impl PlanNode {
    pub fn node_name(&self) -> &'static str {
        match self {
            PlanNode::Query(node) => node.node_name(),
            PlanNode::LogicalJoin(node) => node.node_name(),
            PlanNode::FullKeyAggregate(node) => node.node_name(),
            PlanNode::PreAggregation(node) => node.node_name(),
            PlanNode::ResolveMultipliedMeasures(node) => node.node_name(),
            PlanNode::AggregateMultipliedSubquery(node) => node.node_name(),
            PlanNode::Cube(node) => node.node_name(),
            PlanNode::MeasureSubquery(node) => node.node_name(),
            PlanNode::DimensionSubQuery(node) => node.node_name(),
            PlanNode::KeysSubQuery(node) => node.node_name(),
            PlanNode::MultiStageGetDateRange(node) => node.node_name(),
            PlanNode::MultiStageLeafMeasure(node) => node.node_name(),
            PlanNode::MultiStageMeasureCalculation(node) => node.node_name(),
            PlanNode::MultiStageTimeSeries(node) => node.node_name(),
            PlanNode::MultiStageRollingWindow(node) => node.node_name(),
            PlanNode::LogicalMultiStageMember(node) => node.node_name(),
        }
    }

    pub fn into_logical_node<T: LogicalNode>(self) -> Result<Rc<T>, CubeError> {
        T::try_from_plan_node(self)
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
            "For node {} expected {} inputs but received {}",
            node_type,
            expected,
            inputs.len()
        )))
    }
}
