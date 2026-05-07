use super::operators::{FilterOperationSql, FilterSqlContext};
use super::ToSql;
use crate::cube_bridge::member_sql::FilterParamsColumn;
use crate::plan::sql_nodes::SqlNode;
use crate::plan::SqlEvaluatorVisitor;
use crate::planner::filter::typed_filter::{resolve_base_symbol, FilterOp, TypedFilter};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::FiltersContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

impl ToSql for TypedFilter {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
        filters_ctx: &FiltersContext,
    ) -> Result<String, CubeError> {
        if let FilterOp::MeasureFilter(op) = self.operation() {
            return op.to_sql(
                self.member_evaluator(),
                visitor,
                node_processor,
                query_tools,
                templates,
            );
        }

        let resolved = resolve_base_symbol(self.member_evaluator());
        let member_sql = visitor.apply_for_filter(&resolved, node_processor, templates)?;

        let ctx = FilterSqlContext {
            member_sql: &member_sql,
            query_tools: self.query_tools(),
            plan_templates: templates,
            use_db_time_zone: !filters_ctx.use_local_tz,
            use_raw_values: self.use_raw_values(),
        };

        dispatch_to_sql(self.operation(), &ctx)
    }
}

impl TypedFilter {
    pub fn to_sql_for_filter_params(
        &self,
        column: &FilterParamsColumn,
        plan_templates: &PlanSqlTemplates,
        filters_context: &FiltersContext,
    ) -> Result<String, CubeError> {
        let use_db_time_zone = !filters_context.use_local_tz;

        match column {
            FilterParamsColumn::String(column_sql) => {
                let ctx = FilterSqlContext {
                    member_sql: column_sql,
                    query_tools: self.query_tools(),
                    plan_templates,
                    use_db_time_zone,
                    use_raw_values: self.use_raw_values(),
                };
                dispatch_to_sql(self.operation(), &ctx)
            }
            FilterParamsColumn::Callback(callback) => {
                let args = match self.operation() {
                    FilterOp::DateRange(_) | FilterOp::DateSingle(_) => {
                        let ctx = FilterSqlContext {
                            member_sql: "",
                            query_tools: self.query_tools(),
                            plan_templates,
                            use_db_time_zone,
                            use_raw_values: self.use_raw_values(),
                        };
                        let from = self
                            .values()
                            .first()
                            .and_then(|v| v.as_ref())
                            .map(|v| ctx.format_and_allocate_from_date(v))
                            .transpose()?;
                        let to = self
                            .values()
                            .get(1)
                            .and_then(|v| v.as_ref())
                            .map(|v| ctx.format_and_allocate_to_date(v))
                            .transpose()?;
                        [from, to].into_iter().flatten().collect()
                    }
                    _ => self
                        .values()
                        .iter()
                        .filter_map(|v| v.as_ref().map(|v| self.query_tools().allocate_param(v)))
                        .collect::<Vec<_>>(),
                };
                callback.call(&args)
            }
        }
    }
}

fn dispatch_to_sql(op: &FilterOp, ctx: &FilterSqlContext) -> Result<String, CubeError> {
    match op {
        FilterOp::Comparison(op) => op.to_sql(ctx),
        FilterOp::DateRange(op) => op.to_sql(ctx),
        FilterOp::DateSingle(op) => op.to_sql(ctx),
        FilterOp::Equality(op) => op.to_sql(ctx),
        FilterOp::InList(op) => op.to_sql(ctx),
        FilterOp::Like(op) => op.to_sql(ctx),
        FilterOp::MeasureFilter(_) => {
            unreachable!("MeasureFilter is handled in TypedFilter::to_sql")
        }
        FilterOp::Nullability(op) => op.to_sql(ctx),
        FilterOp::RegularRollingWindow(op) => op.to_sql(ctx),
        FilterOp::ToDateRollingWindow(op) => op.to_sql(ctx),
    }
}
