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
        // A segment can be named by more than one binding of the same group —
        // its own path and the path a view re-exports it under both resolve
        // here. The map is unordered, so the name is what picks between them.
        if let Some(item) = filters_ctx
            .filter_params_columns
            .iter()
            .filter(|(name, _)| self.matches_member_name(name))
            .min_by_key(|(name, _)| *name)
            .map(|(_, item)| item)
        {
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
    // A segment restricts the rows without comparing anything to a filter value,
    // so its `FILTER_PARAMS` column is the whole predicate rather than the left
    // side of one, and no value is passed to a callback column.
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
            FilterParamsColumn::Compiled(compiled) => {
                if compiled.value_params_count > 0 {
                    return Err(CubeError::user(format!(
                        "FILTER_PARAMS column for segment `{}` takes {} filter value(s), but a \
                         segment carries none — declare the callback without parameters",
                        item.filter_symbol_name, compiled.value_params_count
                    )));
                }
                let Some(call) = &item.compiled_call else {
                    return Err(CubeError::internal(format!(
                        "Compiled filter params column for `{}` has no call",
                        item.filter_symbol_name
                    )));
                };
                call.eval(visitor, node_processor, query_tools, templates)
            }
            FilterParamsColumn::Callback(callback) => callback.call(&Vec::new()),
        }
    }
}
