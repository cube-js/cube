use super::ToSql;
use crate::physical_plan::VisitorContext;
use crate::planner::filter::{Filter, FilterItem};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;

pub fn render_filter(
    context: &VisitorContext,
    filter: &Filter,
    templates: &PlanSqlTemplates,
) -> Result<String, CubeError> {
    let visitor = context.make_visitor(context.query_tools());
    filter.to_sql(
        &visitor,
        context.node_processor(),
        context.query_tools(),
        templates,
        context.filters_context(),
    )
}

pub fn render_filter_item(
    context: &VisitorContext,
    item: &FilterItem,
    templates: &PlanSqlTemplates,
) -> Result<String, CubeError> {
    let visitor = context.make_visitor(context.query_tools());
    item.to_sql(
        &visitor,
        context.node_processor(),
        context.query_tools(),
        templates,
        context.filters_context(),
    )
}
