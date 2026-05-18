use super::ToSql;
use crate::physical_plan::sql_nodes::NodeProcessor;
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
        node_processor: Rc<NodeProcessor>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
        filters_ctx: &FiltersContext,
    ) -> Result<String, CubeError> {
        if !filters_ctx.filter_params_columns.is_empty() {
            let symbol_to_match =
                resolve_base_symbol(self.raw_member_evaluator_ref()).resolve_reference_chain();
            if let Some(filter_params_column) = filters_ctx
                .filter_params_columns
                .get(&symbol_to_match.full_name())
            {
                return self.typed_filter().to_sql_for_filter_params(
                    filter_params_column,
                    templates,
                    filters_ctx,
                );
            }
        }
        self.typed_filter()
            .to_sql(visitor, node_processor, query_tools, templates, filters_ctx)
    }
}
