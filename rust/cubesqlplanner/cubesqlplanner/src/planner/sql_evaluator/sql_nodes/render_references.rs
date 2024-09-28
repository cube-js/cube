use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct RenderReferencesSqlNode {
    references: HashMap<String, String>,
    input: Rc<dyn SqlNode>,
}

impl RenderReferencesSqlNode {
    pub fn new(references: HashMap<String, String>, input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { references, input })
    }
}

impl SqlNode for RenderReferencesSqlNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let reference = match node.symbol() {
            MemberSymbolType::Dimension(ev) => self.references.get(&ev.full_name()).cloned(),
            MemberSymbolType::Measure(ev) => self.references.get(&ev.full_name()).cloned(),
            _ => None,
        };

        if let Some(reference) = reference {
            Ok(reference)
        } else {
            self.input.to_sql(visitor, node, query_tools.clone())
        }
    }
}
