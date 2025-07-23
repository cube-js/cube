use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait NodeInputs {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_>;
}

pub struct SingleNodeInput {
    item: PlanNode,
}

impl SingleNodeInput {
    pub fn new(item: PlanNode) -> Self {
        Self { item }
    }

    pub fn item(&self) -> &PlanNode {
        &self.item
    }
}

impl NodeInputs for SingleNodeInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(std::iter::once(&self.item))
    }
}

pub struct OptionNodeInput {
    item: Option<PlanNode>,
}

impl OptionNodeInput {
    pub fn new<T: LogicalNode>(item: Option<PlanNode>) -> Self {
        Self { item }
    }

    pub fn item(&self) -> &Option<PlanNode> {
        &self.item
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
}

pub struct VecNodeInput {
    items: Vec<PlanNode>,
}

impl VecNodeInput {
    pub fn new(items: Vec<PlanNode>) -> Self {
        Self { items }
    }

    pub fn items(&self) -> &Vec<PlanNode> {
        &self.items
    }
}

impl NodeInputs for VecNodeInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(self.items.iter())
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
}

impl PlanNode {
    pub fn node_name(&self) -> &'static str {
        match self {
            PlanNode::Query(_) => Query::node_name(),
            PlanNode::LogicalJoin(_) => LogicalJoin::node_name(),
            PlanNode::FullKeyAggregate(_) => FullKeyAggregate::node_name(),
            PlanNode::PreAggregation(_) => PreAggregation::node_name(),
        }
    }
}

pub(super) fn cast_error<T: LogicalNode>(plan_node: &PlanNode) -> CubeError {
    CubeError::internal(format!(
        "Can't cast {} PlanNode into {}",
        plan_node.node_name(),
        T::node_name(),
    ))
}
