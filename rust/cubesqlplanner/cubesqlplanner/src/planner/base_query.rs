use super::query_tools::QueryTools;
use super::top_level_planner::TopLevelPlanner;
use super::QueryProperties;
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::pre_aggregation_obj::NativePreAggregationObj;
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
            options.security_context()?,
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

    pub fn build_sql_and_params(&self) -> Result<NativeObjectHandle<IT>, CubeError> {
        let planner = TopLevelPlanner::new(
            self.request.clone(),
            self.query_tools.clone(),
            self.cubestore_support_multistage,
        );

        let (sql, used_pre_aggregations) = planner.plan()?;

        let is_external = if !used_pre_aggregations.is_empty() {
            used_pre_aggregations
                .iter()
                .all(|pre_aggregation| pre_aggregation.external())
        } else {
            false
        };

        let templates = self.query_tools.plan_sql_templates(is_external)?;
        let (result_sql, params) = self
            .query_tools
            .build_sql_and_params(&sql, true, &templates)?;

        let res = self.context.empty_array()?;
        res.set(0, result_sql.to_native(self.context.clone())?)?;
        res.set(1, params.to_native(self.context.clone())?)?;
        if let Some(used_pre_aggregation) = used_pre_aggregations.first() {
            let pre_aggregation_obj = self.query_tools.base_tools().get_pre_aggregation_by_name(
                used_pre_aggregation.cube_name().clone(),
                used_pre_aggregation.name().clone(),
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
}
