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
    ungrouped: bool,
}

impl MaskedSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self {
            input,
            ungrouped: false,
        })
    }

    pub fn new_ungrouped(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self {
            input,
            ungrouped: true,
        })
    }

    fn resolve_mask(
        &self,
        node: &Rc<MemberSymbol>,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<Option<String>, CubeError> {
        let full_name = node.full_name();
        if !query_tools.is_member_masked(&full_name) {
            return Ok(None);
        }
        if let Some(mask_call) = node.mask_sql() {
            // In ungrouped mode, skip SQL masks (has deps) on measures
            // since they reference aggregated columns not meaningful per-row.
            if self.ungrouped {
                if let MemberSymbol::Measure(_) = node.as_ref() {
                    if mask_call.dependencies_count() > 0 {
                        return Ok(None);
                    }
                }
            }
            Ok(Some(mask_call.eval(
                visitor,
                node_processor,
                query_tools,
                templates,
            )?))
        } else {
            Ok(Some("(NULL)".to_string()))
        }
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
        if let Some(masked) = self.resolve_mask(
            node,
            visitor,
            node_processor.clone(),
            query_tools.clone(),
            templates,
        )? {
            return Ok(masked);
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
