use super::ToSql;
use crate::cube_bridge::member_sql::FilterParamsColumn;
use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::filter::BaseSegment;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_call::SqlCallFilterParamsItem;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::FiltersContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

impl ToSql for BaseSegment {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
        filters_ctx: &FiltersContext,
    ) -> Result<String, CubeError> {
        if let Some(item) = self.matching_filter_params_column(filters_ctx) {
            return self.filter_params_column_sql(
                item,
                visitor,
                node_processor,
                query_tools,
                templates,
            );
        }

        let sql = visitor.apply(&self.member_evaluator(), node_processor, templates)?;
        if filters_ctx.reading_pre_aggregation {
            // The segment is a stored pre-aggregation column; compare it to its
            // truthy value so dialects without a bare-boolean predicate work.
            templates.wrap_segment_filter(sql)
        } else {
            Ok(sql)
        }
    }
}

impl BaseSegment {
    // The binding named by the path the query asked for is the one the model
    // meant. A view re-exporting a segment leaves only the underlying cube's
    // path to match, which takes a scan; the name breaks the tie when a group
    // binds both paths, since the map is unordered.
    fn matching_filter_params_column<'a>(
        &self,
        filters_ctx: &'a FiltersContext,
    ) -> Option<&'a SqlCallFilterParamsItem> {
        if let Some(item) = filters_ctx.filter_params_columns.get(&self.full_name()) {
            return Some(item);
        }
        filters_ctx
            .filter_params_columns
            .iter()
            .filter(|(name, _)| self.matches_member_name(name))
            .min_by_key(|(name, _)| *name)
            .map(|(_, item)| item)
    }

    // A segment restricts the rows without comparing anything to a filter value,
    // so its `FILTER_PARAMS` column is the whole predicate rather than the left
    // side of one.
    fn filter_params_column_sql(
        &self,
        item: &SqlCallFilterParamsItem,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match &item.column {
            FilterParamsColumn::String(column_sql) => Ok(column_sql.clone()),
            FilterParamsColumn::Compiled(compiled) if compiled.value_params_count == 0 => {
                let Some(call) = &item.compiled_call else {
                    return Err(CubeError::internal(format!(
                        "Compiled filter params column for `{}` has no call",
                        item.filter_symbol_name
                    )));
                };
                call.eval(visitor, node_processor, query_tools, templates)
            }
            // A column that takes filter values cannot render for a segment,
            // which supplies none — a rest parameter consumes as many as the
            // query happens to give, and a parameter list that could not be read
            // is assumed to take some. Only the restatement inside this SQL is
            // dropped; the segment still reaches the query on its own.
            FilterParamsColumn::Compiled(_) | FilterParamsColumn::Callback(_) => {
                templates.always_true()
            }
        }
    }
}
