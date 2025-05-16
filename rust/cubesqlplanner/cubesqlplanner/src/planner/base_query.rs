use super::planners::QueryPlanner;
use super::query_tools::QueryTools;
use super::QueryProperties;
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::pre_aggregation_obj::{NativePreAggregationObj, PreAggregationObj};
use crate::logical_plan::optimizers::*;
use crate::logical_plan::Query;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::object::NativeArray;
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::NativeType;
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle, NativeStruct};
use cubenativeutils::{CubeError, CubeErrorCauseType};
use std::collections::HashMap;
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
            options.static_data().export_annotated_sql,
        )?;

        let request = QueryProperties::try_new(query_tools.clone(), options)?;

        Ok(Self {
            context,
            query_tools,
            request,
        })
    }

    pub fn build_sql_and_params(&self) -> NativeObjectHandle<IT> {
        let build_result = self.build_sql_and_params_impl();
        let result = self.context.empty_struct().unwrap();
        match build_result {
            Ok(res) => {
                result.set_field("result", res).unwrap();
            }
            Err(e) => {
                let error_descr = self.context.empty_struct().unwrap();
                let error_cause = match &e.cause {
                    CubeErrorCauseType::User(_) => "User",
                    CubeErrorCauseType::Internal(_) => "Internal",
                };
                error_descr
                    .set_field(
                        "message",
                        e.message.to_native(self.context.clone()).unwrap(),
                    )
                    .unwrap();
                error_descr
                    .set_field(
                        "cause",
                        error_cause.to_native(self.context.clone()).unwrap(),
                    )
                    .unwrap();
                result
                    .set_field("error", NativeObjectHandle::new(error_descr.into_object()))
                    .unwrap();
            }
        }

        NativeObjectHandle::new(result.into_object())
    }

    fn build_sql_and_params_impl(&self) -> Result<NativeObjectHandle<IT>, CubeError> {
        let templates = self.query_tools.plan_sql_templates();
        let query_planner = QueryPlanner::new(self.request.clone(), self.query_tools.clone());
        let logical_plan = query_planner.plan()?;

        let (optimized_plan, used_pre_aggregations) =
            self.try_pre_aggregations(logical_plan.clone())?;

        let physical_plan_builder = PhysicalPlanBuilder::new(self.query_tools.clone());
        let original_sql_pre_aggregations = if !self.request.is_pre_aggregation_query() {
            OriginalSqlCollector::new(self.query_tools.clone()).collect(&optimized_plan)?
        } else {
            HashMap::new()
        };
        let physical_plan = physical_plan_builder.build(
            optimized_plan,
            original_sql_pre_aggregations,
            self.request.is_total_query(),
        )?;

        let sql = physical_plan.to_sql(&templates)?;
        let (result_sql, params) = self.query_tools.build_sql_and_params(&sql, true)?;

        let res = self.context.empty_array()?;
        res.set(0, result_sql.to_native(self.context.clone())?)?;
        res.set(1, params.to_native(self.context.clone())?)?;
        if let Some(used_pre_aggregations) = used_pre_aggregations.first() {
            res.set(
                2,
                used_pre_aggregations
                    .clone()
                    .as_any()
                    .downcast::<NativePreAggregationObj<IT>>()
                    .unwrap()
                    .to_native(self.context.clone())?,
            )?;
        }
        let result = NativeObjectHandle::new(res.into_object());

        Ok(result)
    }

    fn try_pre_aggregations(
        &self,
        plan: Rc<Query>,
    ) -> Result<(Rc<Query>, Vec<Rc<dyn PreAggregationObj>>), CubeError> {
        let result = if !self.request.is_pre_aggregation_query() {
            let mut pre_aggregation_optimizer =
                PreAggregationOptimizer::new(self.query_tools.clone());
            if let Some(result) = pre_aggregation_optimizer.try_optimize(plan.clone())? {
                if pre_aggregation_optimizer.get_used_pre_aggregations().len() == 1 {
                    (
                        result,
                        pre_aggregation_optimizer.get_used_pre_aggregations(),
                    )
                } else {
                    //TODO multiple pre-aggregations sources required changes in BaseQuery
                    (plan.clone(), Vec::new())
                }
            } else {
                (plan.clone(), Vec::new())
            }
        } else {
            (plan.clone(), Vec::new())
        };
        Ok(result)
    }
}
