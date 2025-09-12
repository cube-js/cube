use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

logical_source_enum!(
    QuerySource,
    [LogicalJoin, FullKeyAggregate, PreAggregation]
);

/* #[derive(Clone)]
pub enum QuerySource {
    CalcGroupsCrossJoin(Rc<CalcGroupsCrossJoin>),
    BaseSource(BaseQuerySource),
}

impl QuerySource {
    pub fn base_source(&self) -> &BaseQuerySource {
        match &self {
            QuerySource::CalcGroupsCrossJoin(cross_join) => cross_join.source(),
            QuerySource::BaseSource(base) => base,
        }
    }
}

impl LogicalSource for QuerySource {
    fn as_plan_node(&self) -> PlanNode {
        match &self {
            QuerySource::CalcGroupsCrossJoin(item) => item.as_plan_node(),
            QuerySource::BaseSource(base) => base.as_plan_node(),
        }
    }
    fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError> {
        Ok(match &self {
            QuerySource::CalcGroupsCrossJoin(_) => {
                QuerySource::CalcGroupsCrossJoin(plan_node.into_logical_node()?)
            }
            QuerySource::BaseSource(base) => {
                QuerySource::BaseSource(base.with_plan_node(plan_node)?)
            }
        })
    }
}

impl PrettyPrint for QuerySource {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match &self {
            QuerySource::CalcGroupsCrossJoin(item) => item.pretty_print(result, state),
            QuerySource::BaseSource(base) => base.pretty_print(result, state),
        }
    }
}

impl From<Rc<CalcGroupsCrossJoin>> for QuerySource {
    fn from(value: Rc<CalcGroupsCrossJoin>) -> Self {
        Self::CalcGroupsCrossJoin(value)
    }
}

impl From<Rc<LogicalJoin>> for QuerySource {
    fn from(value: Rc<LogicalJoin>) -> Self {
        Self::BaseSource(value.into())
    }
}

impl From<Rc<FullKeyAggregate>> for QuerySource {
    fn from(value: Rc<FullKeyAggregate>) -> Self {
        Self::BaseSource(value.into())
    }
}

impl From<Rc<PreAggregation>> for QuerySource {
    fn from(value: Rc<PreAggregation>) -> Self {
        Self::BaseSource(value.into())
    }
} */
