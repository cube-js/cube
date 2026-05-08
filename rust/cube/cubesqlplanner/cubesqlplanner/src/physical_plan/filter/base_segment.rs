use super::ToSql;
use crate::physical_plan::sql_nodes::NodeProcessor;
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
        node_processor: Rc<NodeProcessor>,
        _query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
        _filters_ctx: &FiltersContext,
    ) -> Result<String, CubeError> {
        visitor.apply(&self.member_evaluator(), node_processor, templates)
    }
}
