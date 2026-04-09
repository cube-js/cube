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
use serde::Serialize;
use std::rc::Rc;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PreAggregationUsageInfo {
    cube_name: String,
    pre_aggregation_name: String,
    placeholder: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    date_range: Option<Vec<String>>,
    external: bool,
}

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
            options
                .static_data()
                .convert_tz_for_raw_time_dimension
                .unwrap_or(false),
            options.static_data().masked_members.clone(),
            options.static_data().member_to_alias.clone(),
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

        let (sql, usages) = planner.plan()?;

        let is_external = if !usages.is_empty() {
            usages
                .iter()
                .all(|usage| usage.pre_aggregation.external())
        } else {
            false
        };

        let templates = self.query_tools.plan_sql_templates(is_external)?;
        let (result_sql, params) = self
            .query_tools
            .build_sql_and_params(&sql, true, &templates)?;

        let needs_usage_suffix = usages.len() > 1;

        // For single usage, strip __usage_N suffix from SQL to maintain backward compat
        let final_sql = if !needs_usage_suffix && usages.len() == 1 {
            result_sql.replace(
                &format!("__usage_{}", usages[0].index),
                "",
            )
        } else {
            result_sql
        };

        let res = self.context.empty_array()?;
        res.set(0, final_sql.to_native(self.context.clone())?)?;
        res.set(1, params.to_native(self.context.clone())?)?;

        if needs_usage_suffix {
            // Multiple usages: return array of usage info objects (new format)
            let base_tools = self.query_tools.base_tools();
            let usages_info: Vec<PreAggregationUsageInfo> = usages
                .iter()
                .map(|usage| {
                    let pre_agg = &usage.pre_aggregation;
                    let name = pre_agg.name().clone();
                    let cube_name = pre_agg.cube_name().clone();
                    let base_table = base_tools
                        .pre_aggregation_table_name(cube_name.clone(), name.clone())
                        .unwrap_or_default();
                    PreAggregationUsageInfo {
                        cube_name,
                        pre_aggregation_name: name,
                        placeholder: format!("{}__usage_{}", base_table, usage.index),
                        date_range: usage
                            .date_range
                            .as_ref()
                            .map(|(from, to)| vec![from.clone(), to.clone()]),
                        external: pre_agg.external(),
                    }
                })
                .collect();
            res.set(2, usages_info.to_native(self.context.clone())?)?;
        } else if let Some(usage) = usages.first() {
            // Single usage: return old-style pre-aggregation object for backward compat
            let pre_aggregation_obj = self.query_tools.base_tools().get_pre_aggregation_by_name(
                usage.pre_aggregation.cube_name().clone(),
                usage.pre_aggregation.name().clone(),
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
