use crate::cube_bridge::base_query_options::BaseQueryOptionsStatic;
use crate::planner::planners::multi_stage::{
    DEFAULT_MAX_MULTI_STAGE_DEPTH, DEFAULT_MAX_MULTI_STAGE_STAGES,
};

/// Options fixed for a whole planner run: they shape how the request is planned
/// and rendered, not what any single (sub)query selects.
#[derive(Clone, Debug)]
pub struct RequestOptions {
    // Match pre-aggregations but skip the outer SQL: the refresh path lacks the date
    // range a rolling-window time series in that outer query would need.
    pre_aggregations_match_only: bool,
    // A rollup build reads the cube's `originalSql` pre-aggregation, not the raw cube SQL.
    use_original_sql_pre_aggregations_in_pre_aggregation: bool,
    total_query: bool,
    // Folds multi-stage leaves that read one rollup under different filters into one scan.
    multi_stage_leaf_merge: bool,
    // Lets Cube Store combine multi-stage results with `UNION ALL` instead of joins.
    cubestore_union_full_key_aggregate: bool,
    // The same for the data source's own dialect, when it opts in via its templates.
    union_full_key_aggregate: bool,
    // Multi-stage members one dependency path may carry before planning refuses the query.
    max_multi_stage_depth: usize,
    // Multi-stage stages one multi-stage planning may produce before it refuses the query.
    max_multi_stage_stages: usize,
    disable_external_pre_aggregations: bool,
    pre_aggregation_id: Option<String>,
    cubestore_support_multistage: bool,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            pre_aggregations_match_only: false,
            use_original_sql_pre_aggregations_in_pre_aggregation: false,
            total_query: false,
            multi_stage_leaf_merge: true,
            cubestore_union_full_key_aggregate: true,
            union_full_key_aggregate: true,
            max_multi_stage_depth: DEFAULT_MAX_MULTI_STAGE_DEPTH,
            max_multi_stage_stages: DEFAULT_MAX_MULTI_STAGE_STAGES,
            disable_external_pre_aggregations: false,
            pre_aggregation_id: None,
            cubestore_support_multistage: false,
        }
    }
}

impl RequestOptions {
    pub fn from_static_data(static_data: &BaseQueryOptionsStatic) -> Self {
        let default = Self::default();
        Self {
            pre_aggregations_match_only: static_data
                .pre_aggregations_match_only
                .unwrap_or(default.pre_aggregations_match_only),
            use_original_sql_pre_aggregations_in_pre_aggregation: static_data
                .use_original_sql_pre_aggregations_in_pre_aggregation
                .unwrap_or(default.use_original_sql_pre_aggregations_in_pre_aggregation),
            total_query: static_data.total_query.unwrap_or(default.total_query),
            multi_stage_leaf_merge: static_data
                .multi_stage_leaf_merge
                .unwrap_or(default.multi_stage_leaf_merge),
            cubestore_union_full_key_aggregate: static_data
                .cubestore_union_full_key_aggregate
                .unwrap_or(default.cubestore_union_full_key_aggregate),
            union_full_key_aggregate: static_data
                .union_full_key_aggregate
                .unwrap_or(default.union_full_key_aggregate),
            max_multi_stage_depth: static_data
                .max_multi_stage_depth
                .unwrap_or(default.max_multi_stage_depth),
            max_multi_stage_stages: static_data
                .max_multi_stage_stages
                .unwrap_or(default.max_multi_stage_stages),
            disable_external_pre_aggregations: static_data.disable_external_pre_aggregations,
            pre_aggregation_id: static_data.pre_aggregation_id.clone(),
            cubestore_support_multistage: static_data
                .cubestore_support_multistage
                .unwrap_or(default.cubestore_support_multistage),
        }
    }

    #[cfg(test)]
    pub fn with_cubestore_support_multistage(mut self, value: bool) -> Self {
        self.cubestore_support_multistage = value;
        self
    }

    pub fn is_pre_aggregations_match_only(&self) -> bool {
        self.pre_aggregations_match_only
    }

    pub fn use_original_sql_pre_aggregations_in_pre_aggregation(&self) -> bool {
        self.use_original_sql_pre_aggregations_in_pre_aggregation
    }

    pub fn is_total_query(&self) -> bool {
        self.total_query
    }

    pub fn multi_stage_leaf_merge(&self) -> bool {
        self.multi_stage_leaf_merge
    }

    pub fn cubestore_union_full_key_aggregate(&self) -> bool {
        self.cubestore_union_full_key_aggregate
    }

    pub fn union_full_key_aggregate(&self) -> bool {
        self.union_full_key_aggregate
    }

    pub fn max_multi_stage_depth(&self) -> usize {
        self.max_multi_stage_depth
    }

    pub fn max_multi_stage_stages(&self) -> usize {
        self.max_multi_stage_stages
    }

    pub fn disable_external_pre_aggregations(&self) -> bool {
        self.disable_external_pre_aggregations
    }

    pub fn pre_aggregation_id(&self) -> Option<&str> {
        self.pre_aggregation_id.as_deref()
    }

    pub fn cubestore_support_multistage(&self) -> bool {
        self.cubestore_support_multistage
    }
}
