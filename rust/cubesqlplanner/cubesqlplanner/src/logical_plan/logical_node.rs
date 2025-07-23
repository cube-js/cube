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
    type InputsType: NodeInputs;

    fn inputs(&self) -> Self::InputsType;

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError>;

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError>;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode;

    fn node_name() -> &'static str;
}

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
            PlanNode::Query(_) => Query::node_name(),
            PlanNode::LogicalJoin(_) => LogicalJoin::node_name(),
            PlanNode::FullKeyAggregate(_) => FullKeyAggregate::node_name(),
            PlanNode::PreAggregation(_) => PreAggregation::node_name(),
            PlanNode::ResolveMultipliedMeasures(_) => ResolveMultipliedMeasures::node_name(),
            PlanNode::AggregateMultipliedSubquery(_) => AggregateMultipliedSubquery::node_name(),
            PlanNode::Cube(_) => Cube::node_name(),
            PlanNode::MeasureSubquery(_) => MeasureSubquery::node_name(),
            PlanNode::DimensionSubQuery(_) => DimensionSubQuery::node_name(),
            PlanNode::KeysSubQuery(_) => KeysSubQuery::node_name(),
            PlanNode::MultiStageGetDateRange(_) => MultiStageGetDateRange::node_name(),
            PlanNode::MultiStageLeafMeasure(_) => MultiStageLeafMeasure::node_name(),
            PlanNode::MultiStageMeasureCalculation(_) => MultiStageMeasureCalculation::node_name(),
            PlanNode::MultiStageTimeSeries(_) => MultiStageTimeSeries::node_name(),
            PlanNode::MultiStageRollingWindow(_) => MultiStageRollingWindow::node_name(),
            PlanNode::LogicalMultiStageMember(_) => LogicalMultiStageMember::node_name(),
        }
    }

    pub fn into_logical_node<T: LogicalNode>(self) -> Result<Rc<T>, CubeError> {
        T::try_from_plan_node(self)
    }
}

pub(super) fn cast_error<T: LogicalNode>(plan_node: &PlanNode) -> CubeError {
    CubeError::internal(format!(
        "Can't cast {} PlanNode into {}",
        plan_node.node_name(),
        T::node_name(),
    ))
}

pub(super) fn check_inputs_len<T: LogicalNode>(
    input_name: &str,
    inputs: &Vec<PlanNode>,
    expected: usize,
) -> Result<(), CubeError> {
    if inputs.len() == expected {
        Ok(())
    } else {
        Err(CubeError::internal(format!(
            "For input {} for node {} expected {} inputs but received {}",
            input_name,
            T::node_name(),
            expected,
            inputs.len()
        )))
    }
}
