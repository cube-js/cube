use super::{
    FullKeyAggregateQueryPlanner, MultiStageQueryPlanner, MultipliedMeasuresQueryPlanner,
    SimpleQueryPlanner,
};
use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct QueryPlanner {
    query_tools: Rc<QueryTools>,
    request: Rc<QueryProperties>,
}

impl QueryPlanner {
    pub fn new(request: Rc<QueryProperties>, query_tools: Rc<QueryTools>) -> Self {
        Self {
            request,
            query_tools,
        }
    }

    pub fn plan(&self) -> Result<Rc<Query>, CubeError> {
        if self.request.is_simple_query()? {
            let planner = SimpleQueryPlanner::new(self.query_tools.clone(), self.request.clone());
            planner.plan()
        } else {
            let request = self.request.clone();
            let multiplied_measures_query_planner =
                MultipliedMeasuresQueryPlanner::try_new(self.query_tools.clone(), request.clone())?;
            let multi_stage_query_planner =
                MultiStageQueryPlanner::new(self.query_tools.clone(), request.clone());
            let full_key_aggregate_planner = FullKeyAggregateQueryPlanner::new(request.clone());
            let multiplied_resolver = multiplied_measures_query_planner.plan_queries()?;
            let (multi_stage_members, multi_stage_refs) =
                multi_stage_query_planner.plan_queries()?;

            let result = full_key_aggregate_planner.plan_logical_plan(
                Some(multiplied_resolver),
                multi_stage_refs,
                multi_stage_members,
            )?;

            Ok(result)
        }
    }
}
