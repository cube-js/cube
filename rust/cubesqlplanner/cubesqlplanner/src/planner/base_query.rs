use super::planners::{
    FullKeyAggregateQueryPlanner, MultiStageQueryPlanner, MultipliedMeasuresQueryPlanner,
    SimpleQueryPlanner,
};
use super::query_tools::QueryTools;
use super::QueryProperties;
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::plan::Select;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::object::NativeArray;
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::NativeType;
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseQuery<IT: InnerTypes> {
    context: NativeContextHolder<IT>,
    query_tools: Rc<QueryTools>,
    request: Rc<QueryProperties>,
}

impl<IT: InnerTypes> BaseQuery<IT> {
    pub fn try_new(
        context: NativeContextHolder<IT>,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Self, CubeError> {
        let query_tools = QueryTools::try_new(
            options.cube_evaluator()?,
            options.base_tools()?,
            options.join_graph()?,
            options.static_data().timezone.clone(),
        )?;

        let request = QueryProperties::try_new(query_tools.clone(), options)?;

        Ok(Self {
            context,
            query_tools,
            request,
        })
    }

    pub fn build_sql_and_params(&self) -> Result<NativeObjectHandle<IT>, CubeError> {
        let plan = self.build_sql_and_params_impl()?;
        let templates = PlanSqlTemplates::new(self.query_tools.templates_render());

        let sql = plan.to_sql(&templates)?;
        let (result_sql, params) = self.query_tools.build_sql_and_params(&sql, true)?;

        let res = self.context.empty_array();
        res.set(0, result_sql.to_native(self.context.clone())?)?;
        res.set(1, params.to_native(self.context.clone())?)?;
        let result = NativeObjectHandle::new(res.into_object());

        Ok(result)
    }

    fn build_sql_and_params_impl(&self) -> Result<Select, CubeError> {
        if self.request.is_simple_query()? {
            let planner = SimpleQueryPlanner::new(
                self.query_tools.clone(),
                self.request.clone(),
                SqlNodesFactory::new(),
            );
            planner.plan()
        } else {
            let multiplied_measures_query_planner = MultipliedMeasuresQueryPlanner::new(
                self.query_tools.clone(),
                self.request.clone(),
                SqlNodesFactory::new(),
            );
            let multi_stage_query_planner =
                MultiStageQueryPlanner::new(self.query_tools.clone(), self.request.clone());
            let full_key_aggregate_planner =
                FullKeyAggregateQueryPlanner::new(self.request.clone(), SqlNodesFactory::new());
            let mut subqueries = multiplied_measures_query_planner.plan_queries()?;
            let (multi_stage_ctes, multi_stage_subqueries) =
                multi_stage_query_planner.plan_queries()?;
            subqueries.extend(multi_stage_subqueries.into_iter());
            let result = full_key_aggregate_planner.plan(subqueries, multi_stage_ctes)?;
            Ok(result)
        }
    }
}
