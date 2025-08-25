use super::planners::QueryPlanner;
use super::query_tools::QueryTools;
use super::QueryProperties;
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::pre_aggregation_obj::NativePreAggregationObj;
use crate::logical_plan::OriginalSqlCollector;
//use crate::logical_plan::optimizers::*;
use crate::logical_plan::PreAggregation;
use crate::logical_plan::PreAggregationOptimizer;
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
    cubestore_support_multistage: bool,
}

impl<IT: InnerTypes> BaseQuery<IT> {
    pub fn try_new(
        context: NativeContextHolder<IT>,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Self, CubeError> {
        let cubestore_support_multistage = options
            .static_data()
            .cubestore_support_multistage
            .unwrap_or(false);
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
            cubestore_support_multistage,
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
        let query_planner = QueryPlanner::new(self.request.clone(), self.query_tools.clone());
        let logical_plan = query_planner.plan()?;

        let (optimized_plan, used_pre_aggregations) =
            self.try_pre_aggregations(logical_plan.clone())?;

        let is_external = if !used_pre_aggregations.is_empty() {
            used_pre_aggregations
                .iter()
                .all(|pre_aggregation| pre_aggregation.external)
        } else {
            false
        };

        let templates = self.query_tools.plan_sql_templates(is_external)?;

        let physical_plan_builder =
            PhysicalPlanBuilder::new(self.query_tools.clone(), templates.clone());
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
        let (result_sql, params) = self
            .query_tools
            .build_sql_and_params(&sql, true, &templates)?;

        let res = self.context.empty_array()?;
        res.set(0, result_sql.to_native(self.context.clone())?)?;
        res.set(1, params.to_native(self.context.clone())?)?;
        if let Some(used_pre_aggregation) = used_pre_aggregations.first() {
            //FIXME We should build this object in Rust
            let pre_aggregation_obj = self.query_tools.base_tools().get_pre_aggregation_by_name(
                used_pre_aggregation.cube_name.clone(),
                used_pre_aggregation.name.clone(),
            )?;
            res.set(
                2,
                pre_aggregation_obj
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
    ) -> Result<(Rc<Query>, Vec<Rc<PreAggregation>>), CubeError> {
        let result = if !self.request.is_pre_aggregation_query() {
            let mut pre_aggregation_optimizer = PreAggregationOptimizer::new(
                self.query_tools.clone(),
                self.cubestore_support_multistage,
            );
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
