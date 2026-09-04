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
                // Both shift kinds: `extract_time_shifts` routes calendar
                // shifts to their own map, so `time_shifts` alone misses them.
                let time_shift = visitor
                    .time_shifts()
                    .get_for_symbol(&symbol_to_match)
                    .and_then(|shift| shift.interval.as_ref())
                    .map(FilterParamsTimeShift::Interval)
                    .or_else(|| {
                        // Keyed by the calendar cube's PK, which is the filtered
                        // symbol only when FILTER_PARAMS binds the calendar
                        // dimension itself; a binding on the fact's own time
                        // dimension reaches it via `time_shift_pk_full_name`.
                        // Probe both - a miss here renders the column bare.
                        let calendar_shifts = visitor.calendar_time_shifts();
                        calendar_shifts
                            .get(&symbol_to_match.full_name())
                            .or_else(|| {
                                let pk = symbol_to_match
                                    .as_dimension()
                                    .ok()?
                                    .time_shift_pk_full_name()?;
                                calendar_shifts.get(&pk)
                            })
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
