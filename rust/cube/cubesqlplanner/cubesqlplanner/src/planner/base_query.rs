use super::state::State;
use super::top_level_planner::TopLevelPlanner;
use super::{QueryProperties, QueryPropertiesCompiler};
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::logical_plan::PreAggregationUsage;
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::object::NativeArray;
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::NativeType;
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use serde::Serialize;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageDateRange {
    #[serde(skip_serializing_if = "Option::is_none")]
    date_range: Option<Vec<String>>,
    /// No partition can be ruled out for the usage.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    unbounded: bool,
}

impl UsageDateRange {
    fn from_usage(usage: &PreAggregationUsage) -> Self {
        Self {
            date_range: usage
                .date_range
                .as_ref()
                .map(|(from, to)| vec![from.clone(), to.clone()]),
            unbounded: usage.unbounded,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GroupedPreAggregationInfo {
    cube_name: String,
    pre_aggregation_name: String,
    external: bool,
    usages: HashMap<String, UsageDateRange>,
}

pub struct BaseQuery<IT: InnerTypes> {
    context: NativeContextHolder<IT>,
    query_tools: Rc<State>,
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
        let query_tools = State::try_new(
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
            options.static_data().max_member_resolution_depth,
        )?;

        let request = QueryPropertiesCompiler::new(query_tools.clone()).build(options)?;

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
            usages.iter().all(|usage| usage.pre_aggregation.external())
        } else {
            false
        };

        let templates = self.query_tools.plan_sql_templates(is_external)?;
        let (result_sql, params) = self.query_tools.build_sql_and_params(&sql, &templates)?;

        // A lone usage reads the table under its plain name, as it always has:
        // the name keys `usedPreAggregations` and appears in `/sql` output.
        let single_usage = usages.len() == 1;
        let final_sql = if single_usage {
            result_sql.replace(&format!("__usage_{}", usages[0].index), "")
        } else {
            result_sql
        };

        let res = self.context.empty_array()?;
        res.set(0, final_sql.to_native(self.context.clone())?)?;
        res.set(1, params.to_native(self.context.clone())?)?;

        if !usages.is_empty() {
            // Grouped by (cubeName, name), with the dates each usage reads.
            let grouped = Self::group_usages(&usages, single_usage);
            res.set(2, grouped.to_native(self.context.clone())?)?;
        }

        let result = NativeObjectHandle::new(res.into_object());
        Ok(result)
    }

    /// `unsuffixed` keys the usages by an empty suffix, for SQL whose table
    /// names carry none.
    fn group_usages(
        usages: &[PreAggregationUsage],
        unsuffixed: bool,
    ) -> Vec<GroupedPreAggregationInfo> {
        let mut groups: HashMap<(String, String), GroupedPreAggregationInfo> = HashMap::new();

        for usage in usages {
            let pre_agg = &usage.pre_aggregation;
            let cube_name = pre_agg.cube_name().clone();
            let name = pre_agg.name().clone();
            let key = (cube_name.clone(), name.clone());

            let suffix = if unsuffixed {
                String::new()
            } else {
                format!("__usage_{}", usage.index)
            };

            let group = groups
                .entry(key)
                .or_insert_with(|| GroupedPreAggregationInfo {
                    cube_name,
                    pre_aggregation_name: name,
                    external: pre_agg.external(),
                    usages: HashMap::new(),
                });

            group
                .usages
                .insert(suffix, UsageDateRange::from_usage(usage));
        }

        let mut result: Vec<_> = groups.into_values().collect();
        result.sort_by(|a, b| {
            a.cube_name
                .cmp(&b.cube_name)
                .then(a.pre_aggregation_name.cmp(&b.pre_aggregation_name))
        });
        result
    }
}
