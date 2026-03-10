use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct MaskedSqlNode {
    input: Rc<dyn SqlNode>,
}

impl MaskedSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }
}

impl SqlNode for MaskedSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        // Only mask dimensions (and time dimensions). Measure masking is
        // handled by FinalMeasureSqlNode so it can skip aggregation wrapping.
        // In ungrouped queries measures should not be masked at all.
        match node.as_ref() {
            MemberSymbol::Dimension(_) | MemberSymbol::TimeDimension(_) => {
                if let Some(mask_call) = node.mask_sql() {
                    let full_name = node.full_name();
                    if query_tools.is_member_masked(&full_name) {
                        return mask_call.eval(visitor, node_processor, query_tools, templates);
                    }
                }
            }
            _ => {}
        }
        self.input
            .to_sql(visitor, node, query_tools, node_processor, templates)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
