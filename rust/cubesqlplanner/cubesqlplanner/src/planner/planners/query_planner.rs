use super::{
    FullKeyAggregateQueryPlanner, MultiStageQueryPlanner, MultipliedMeasuresQueryPlanner,
    SimpleQueryPlanner,
};
use crate::plan::Select;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_templates::PlanSqlTemplates;
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

    pub fn build_sql(&self) -> Result<Rc<Select>, CubeError> {
        let templates = PlanSqlTemplates::new(self.query_tools.templates_render());
        self.build_sql_and_params_impl(templates)
    }

    fn build_sql_and_params_impl(
        &self,
        templates: PlanSqlTemplates,
    ) -> Result<Rc<Select>, CubeError> {
        let mut nodes_factory = SqlNodesFactory::new();

        if self.request.ungrouped() {
            nodes_factory.set_ungrouped(true)
        }

        if self.request.is_simple_query()? {
            let planner = SimpleQueryPlanner::new(
                self.query_tools.clone(),
                self.request.clone(),
                nodes_factory.clone(),
            );
            planner.plan()
        } else {
            let request = self.request.clone();
            let multiplied_measures_query_planner = MultipliedMeasuresQueryPlanner::try_new(
                self.query_tools.clone(),
                request.clone(),
                nodes_factory.clone(),
            )?;
            let multi_stage_query_planner =
                MultiStageQueryPlanner::new(self.query_tools.clone(), request.clone());
            let full_key_aggregate_planner = FullKeyAggregateQueryPlanner::new(
                request.clone(),
                nodes_factory.clone(),
                templates,
            );
            let mut subqueries = multiplied_measures_query_planner.plan_queries()?;
            let (multi_stage_ctes, multi_stage_subqueries) =
                multi_stage_query_planner.plan_queries()?;
            subqueries.extend(multi_stage_subqueries.into_iter());
            let result = full_key_aggregate_planner.plan(subqueries, multi_stage_ctes)?;
            Ok(result)
        }
    }
}
