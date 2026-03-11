use std::any::Any;
use std::rc::Rc;

use cubenativeutils::CubeError;
use typed_builder::TypedBuilder;

use crate::{
    cube_bridge::{
        base_query_options::{
            BaseQueryOptions, BaseQueryOptionsStatic, FilterItem, OrderByItem, TimeDimension,
        },
        base_tools::BaseTools,
        evaluator::CubeEvaluator,
        join_graph::JoinGraph,
        join_hints::JoinHintItem,
        options_member::OptionsMember,
        security_context::SecurityContext,
    },
    impl_static_data,
};

/// Mock implementation of BaseQueryOptions for testing
#[derive(TypedBuilder, Clone)]
pub struct MockBaseQueryOptions {
    // Required fields - dependencies that must be provided
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
    join_graph: Rc<dyn JoinGraph>,
    security_context: Rc<dyn SecurityContext>,

    // Optional fields from trait methods
    #[builder(default)]
    measures: Option<Vec<OptionsMember>>,
    #[builder(default)]
    dimensions: Option<Vec<OptionsMember>>,
    #[builder(default)]
    segments: Option<Vec<OptionsMember>>,
    #[builder(default)]
    join_hints: Option<Vec<JoinHintItem>>,

    // Fields from BaseQueryOptionsStatic
    #[builder(default)]
    time_dimensions: Option<Vec<TimeDimension>>,
    #[builder(default)]
    timezone: Option<String>,
    #[builder(default)]
    filters: Option<Vec<FilterItem>>,
    #[builder(default)]
    order: Option<Vec<OrderByItem>>,
    #[builder(default)]
    limit: Option<String>,
    #[builder(default)]
    row_limit: Option<String>,
    #[builder(default)]
    offset: Option<String>,
    #[builder(default)]
    ungrouped: Option<bool>,
    #[builder(default = false)]
    export_annotated_sql: bool,
    #[builder(default)]
    pre_aggregation_query: Option<bool>,
    #[builder(default)]
    total_query: Option<bool>,
    #[builder(default)]
    cubestore_support_multistage: Option<bool>,
    #[builder(default = false)]
    disable_external_pre_aggregations: bool,
    #[builder(default)]
    pre_aggregation_id: Option<String>,
}

impl_static_data!(
    MockBaseQueryOptions,
    BaseQueryOptionsStatic,
    time_dimensions,
    timezone,
    filters,
    order,
    limit,
    row_limit,
    offset,
    ungrouped,
    export_annotated_sql,
    pre_aggregation_query,
    total_query,
    cubestore_support_multistage,
    disable_external_pre_aggregations,
    pre_aggregation_id
);

pub fn members_from_strings<S: ToString>(strings: Vec<S>) -> Vec<OptionsMember> {
    strings
        .into_iter()
        .map(|s| OptionsMember::MemberName(s.to_string()))
        .collect()
}

#[allow(dead_code)]
pub fn filter_item<M: ToString, O: ToString, V: ToString>(
    member: M,
    operator: O,
    values: Vec<V>,
) -> FilterItem {
    FilterItem {
        member: Some(member.to_string()),
        dimension: None,
        operator: Some(operator.to_string()),
        values: Some(values.into_iter().map(|v| Some(v.to_string())).collect()),
        or: None,
        and: None,
    }
}

#[allow(dead_code)]
pub fn filter_or(items: Vec<FilterItem>) -> FilterItem {
    FilterItem {
        or: Some(items),
        member: None,
        dimension: None,
        operator: None,
        values: None,
        and: None,
    }
}

#[allow(dead_code)]
pub fn filter_and(items: Vec<FilterItem>) -> FilterItem {
    FilterItem {
        and: Some(items),
        member: None,
        dimension: None,
        operator: None,
        values: None,
        or: None,
    }
}

impl BaseQueryOptions for MockBaseQueryOptions {
    crate::impl_static_data_method!(BaseQueryOptionsStatic);

    fn has_measures(&self) -> Result<bool, CubeError> {
        Ok(self.measures.is_some())
    }

    fn measures(&self) -> Result<Option<Vec<OptionsMember>>, CubeError> {
        Ok(self.measures.clone())
    }

    fn has_dimensions(&self) -> Result<bool, CubeError> {
        Ok(self.dimensions.is_some())
    }

    fn dimensions(&self) -> Result<Option<Vec<OptionsMember>>, CubeError> {
        Ok(self.dimensions.clone())
    }

    fn has_segments(&self) -> Result<bool, CubeError> {
        Ok(self.segments.is_some())
    }

    fn segments(&self) -> Result<Option<Vec<OptionsMember>>, CubeError> {
        Ok(self.segments.clone())
    }

    fn cube_evaluator(&self) -> Result<Rc<dyn CubeEvaluator>, CubeError> {
        Ok(self.cube_evaluator.clone())
    }

    fn base_tools(&self) -> Result<Rc<dyn BaseTools>, CubeError> {
        Ok(self.base_tools.clone())
    }

    fn join_graph(&self) -> Result<Rc<dyn JoinGraph>, CubeError> {
        Ok(self.join_graph.clone())
    }

    fn security_context(&self) -> Result<Rc<dyn SecurityContext>, CubeError> {
        Ok(self.security_context.clone())
    }

    fn has_join_hints(&self) -> Result<bool, CubeError> {
        Ok(self.join_hints.is_some())
    }

    fn join_hints(&self) -> Result<Option<Vec<JoinHintItem>>, CubeError> {
        Ok(self.join_hints.clone())
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
