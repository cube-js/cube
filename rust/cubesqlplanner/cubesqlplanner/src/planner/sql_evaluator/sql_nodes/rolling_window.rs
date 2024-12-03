use super::SqlNode;
use crate::cube_bridge::memeber_sql::MemberSqlArg;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType, SqlEvaluatorVisitor};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct RollingWindowNode {
    input: Rc<dyn SqlNode>,
}

impl RollingWindowNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for RollingWindowNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Measure(m) => {
                if m.is_cumulative() && m.is_splitted_source() {
                    let args = visitor.evaluate_deps(node, node_processor.clone())?;
                    //FIXME hack for working with
                    //measures like rolling window
                    let input = if !args.is_empty() {
                        match &args[0] {
                            MemberSqlArg::String(s) => s.clone(),
                            _ => "".to_string(),
                        }
                    } else {
                        "".to_string()
                    };
                    let aggregate_function = if m.measure_type() == "sum"
                        || m.measure_type() == "count"
                        || m.measure_type() == "runningTotal"
                    {
                        "sum"
                    } else {
                        m.measure_type()
                    };

                    format!("{}({})", aggregate_function, input)
                } else {
                    self.input
                        .to_sql(visitor, node, query_tools.clone(), node_processor.clone())?
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Unexpected evaluation node type for RollingWindowNode"
                )));
            }
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
