//! DTO layer for talking to the JavaScript side of Cube.
//!
//! Every type here mirrors the shape of an object delivered by the
//! schema compiler — cube and member definitions, filter params,
//! callbacks, security context, and so on. Tesseract reads these
//! types as input; no business logic lives here.

pub mod access_condition_definition;
pub mod access_filter_definition;
pub mod access_policy_definition;
pub mod base_query_options;
pub mod base_tools;
pub mod case_definition;
pub mod case_else_item;
pub mod case_item;
pub mod case_switch_definition;
pub mod case_switch_else_item;
pub mod case_switch_item;
pub mod case_variant;
pub mod cube_definition;
pub mod cube_join_definition;
pub mod dimension_definition;
pub mod driver_tools;
pub mod evaluator;
pub mod filter_group;
pub mod filter_params;
pub mod filter_params_callback;
pub mod geo_item;
pub mod granularity_definition;
pub mod hierarchy_definition;
pub mod join_definition;
pub mod join_graph;
pub mod join_hints;
pub mod join_item;
pub mod join_item_definition;
pub mod measure_definition;
pub mod member_definition;
pub mod member_expression;
pub mod member_level_access_definition;
pub mod member_order_by;
pub mod member_sql;
pub mod multi_stage_filter;
pub mod multi_stage_grain;
pub mod options_member;
pub mod pre_aggregation_description;
pub mod pre_aggregation_index_definition;
pub mod pre_aggregation_obj;
pub mod pre_aggregation_time_dimension;
pub mod refresh_key_definition;
pub mod row_level_access_definition;
pub mod schema_source;
pub mod security_context;
pub mod segment_definition;
pub mod sql_templates_render;
pub mod sql_utils;
pub mod string_or_sql;
pub mod struct_with_sql_member;
pub mod timeshift_definition;
pub mod view_filter_definition;
pub mod view_included_member;
