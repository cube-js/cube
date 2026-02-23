use super::planners::QueryPlanner;
use super::query_tools::QueryTools;
use super::QueryProperties;
use crate::logical_plan::OriginalSqlCollector;
use crate::logical_plan::PreAggregation;
use crate::logical_plan::PreAggregationOptimizer;
use crate::logical_plan::Query;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct TopLevelPlanner {
    query_tools: Rc<QueryTools>,
    request: Rc<QueryProperties>,
    cubestore_support_multistage: bool,
}

impl TopLevelPlanner {
    pub fn new(
        request: Rc<QueryProperties>,
        query_tools: Rc<QueryTools>,
        cubestore_support_multistage: bool,
    ) -> Self {
        Self {
            request,
            query_tools,
            cubestore_support_multistage,
        }
    }

    pub fn plan(&self) -> Result<(String, Vec<Rc<PreAggregation>>), CubeError> {
        let query_planner = QueryPlanner::new(self.request.clone(), self.query_tools.clone());
        let logical_plan = query_planner.plan()?;

        let (optimized_plan, used_pre_aggregations) =
            self.try_pre_aggregations(logical_plan.clone())?;

        let is_external = if !used_pre_aggregations.is_empty() {
            used_pre_aggregations
                .iter()
                .all(|pre_aggregation| pre_aggregation.external())
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

        Ok((sql, used_pre_aggregations))
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
            let disable_external_pre_aggregations =
                self.request.disable_external_pre_aggregations();
            let pre_aggregation_id = self.request.pre_aggregation_id();
            if let Some(result) = pre_aggregation_optimizer.try_optimize(
                plan.clone(),
                disable_external_pre_aggregations,
                pre_aggregation_id,
            )? {
                if pre_aggregation_optimizer.get_used_pre_aggregations().len() == 1 {
                    (
                        result,
                        pre_aggregation_optimizer.get_used_pre_aggregations(),
                    )
                } else {
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
