use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait LogicalSource: Sized {
    fn as_plan_node(&self) -> PlanNode;
    fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError>;
}

/* test example */

pub enum TestSouce {
    Cube(Rc<Cube>),
    MeasureSubquery(Rc<MeasureSubquery>),
}

impl LogicalSource for TestSouce {
    fn as_plan_node(&self) -> PlanNode {
        match self {
            Self::Cube(item) => item.as_plan_node(),
            Self::MeasureSubquery(item) => item.as_plan_node(),
        }
    }
    fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError> {
        Ok(match self {
            Self::Cube(_) => Self::Cube(plan_node.into_logical_node()?),
            Self::MeasureSubquery(_) => Self::MeasureSubquery(plan_node.into_logical_node()?),
        })
    }
}

impl From<Rc<Cube>> for TestSouce {
    fn from(value: Rc<Cube>) -> Self {
        Self::Cube(value)
    }
}

impl From<Rc<MeasureSubquery>> for TestSouce {
    fn from(value: Rc<MeasureSubquery>) -> Self {
        Self::MeasureSubquery(value)
    }
}
