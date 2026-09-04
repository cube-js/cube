use super::ToSql;
use crate::physical_plan::filter::typed_filter::FilterParamsTimeShift;
use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::filter::typed_filter::resolve_base_symbol;
use crate::planner::filter::BaseFilter;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::FiltersContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

impl ToSql for BaseFilter {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
        filters_ctx: &FiltersContext,
    ) -> Result<String, CubeError> {
        if !filters_ctx.filter_params_columns.is_empty() {
            let symbol_to_match =
                resolve_base_symbol(self.raw_member_evaluator_ref()).resolve_reference_chain();
            if let Some(filter_params_item) = filters_ctx
                .filter_params_columns
                .get(&symbol_to_match.full_name())
            {
                // Both shift kinds, not just the interval one. A calendar shift
                // never reaches `time_shifts` - `extract_time_shifts` routes it
                // to its own map - and reading only that map is what used to
                // leave the pushed-down column bare against unshifted bounds
                // inside a calendar-shifted stage.
                let time_shift = visitor
                    .time_shifts()
                    .get_for_symbol(&symbol_to_match)
                    .and_then(|shift| shift.interval.as_ref())
                    .map(FilterParamsTimeShift::Interval)
                    .or_else(|| {
                        visitor
                            .calendar_time_shifts()
                            .get(&symbol_to_match.full_name())
                            .map(FilterParamsTimeShift::Calendar)
                    });
                return self.typed_filter().to_sql_for_filter_params(
                    filter_params_item,
                    time_shift,
                    visitor,
                    node_processor,
                    &query_tools,
                    templates,
                    filters_ctx,
                );
            }
        }
        self.typed_filter()
            .to_sql(visitor, node_processor, query_tools, templates, filters_ctx)
    }
}
