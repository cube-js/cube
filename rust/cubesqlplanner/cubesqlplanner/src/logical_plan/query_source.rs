use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum QuerySource {
    LogicalJoin(Rc<LogicalJoin>),
    FullKeyAggregate(Rc<FullKeyAggregate>),
    PreAggregation(Rc<PreAggregation>),
}

impl QuerySource {
    pub fn as_plan_node(&self) -> PlanNode {
        match self {
            Self::LogicalJoin(item) => item.as_plan_node(),
            Self::FullKeyAggregate(item) => item.as_plan_node(),
            Self::PreAggregation(item) => item.as_plan_node(),
        }
    }
    pub fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError> {
        Ok(match self {
            Self::LogicalJoin(_) => Self::LogicalJoin(plan_node.into_logical_node()?),
            Self::FullKeyAggregate(_) => Self::FullKeyAggregate(plan_node.into_logical_node()?),
            Self::PreAggregation(_) => Self::PreAggregation(plan_node.into_logical_node()?),
        })
    }
}

impl PrettyPrint for QuerySource {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            QuerySource::LogicalJoin(join) => join.pretty_print(result, state),
            QuerySource::FullKeyAggregate(full_key) => full_key.pretty_print(result, state),
            QuerySource::PreAggregation(pre_aggregation) => {
                pre_aggregation.pretty_print(result, state)
            }
        }
    }
}
