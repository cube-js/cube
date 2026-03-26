use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::symbols::MeasureKind;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct MultiStageMedianNode {
    else_processor: Rc<dyn SqlNode>,
}

impl MultiStageMedianNode {
    pub fn new(else_processor: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { else_processor })
    }

    pub fn else_processor(&self) -> &Rc<dyn SqlNode> {
        &self.else_processor
    }
}

impl SqlNode for MultiStageMedianNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(m) => {
                if m.is_multi_stage() && matches!(m.kind(), MeasureKind::Median(_)) {
                    if let MeasureKind::Median(Some(sql_call)) = m.kind() {
                        let inner_sql = sql_call.eval(
                            visitor,
                            node_processor.clone(),
                            query_tools.clone(),
                            templates,
                        )?;
                        format!("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {})", inner_sql)
                    } else {
                        return Err(CubeError::internal(
                            "Median measure requires a SQL expression".to_string(),
                        ));
                    }
                } else {
                    self.else_processor.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?
                }
            }
            _ => {
                return Err(CubeError::internal(
                    "Unexpected evaluation node type for MultiStageMedianNode".to_string(),
                ));
            }
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.else_processor.clone()]
    }
}
