use super::ToSql;
use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::filter::BaseSegment;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::FiltersContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

impl ToSql for BaseSegment {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        _query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
        filters_ctx: &FiltersContext,
    ) -> Result<String, CubeError> {
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
