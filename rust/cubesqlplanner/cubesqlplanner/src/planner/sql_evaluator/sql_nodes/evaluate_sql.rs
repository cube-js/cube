use super::SqlNode;
use crate::cube_bridge::memeber_sql::MemberSqlArg;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct EvaluateSqlNode {}

impl EvaluateSqlNode {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {})
    }
}

impl SqlNode for EvaluateSqlNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        _query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        let args = visitor.evaluate_deps(node, node_processor.clone())?;
        match node.symbol() {
            MemberSymbolType::Dimension(ev) => ev.evaluate_sql(args),
            MemberSymbolType::Measure(ev) => {
                let res = if ev.is_splitted_source() {
                    //FIXME hack for working with
                    //measures like rolling window
                    if !args.is_empty() {
                        match &args[0] {
                            MemberSqlArg::String(s) => s.clone(),
                            _ => "".to_string(),
                        }
                    } else {
                        "".to_string()
                    }
                } else {
                    ev.evaluate_sql(args)?
                };
                Ok(res)
            }
            MemberSymbolType::CubeTable(ev) => ev.evaluate_sql(args),
            MemberSymbolType::CubeName(ev) => ev.evaluate_sql(args),
            MemberSymbolType::SimpleSql(ev) => ev.evaluate_sql(args),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![]
    }
}
