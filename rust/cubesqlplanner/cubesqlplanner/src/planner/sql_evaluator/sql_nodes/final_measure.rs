use super::SqlNode;
use crate::cube_bridge::memeber_sql::MemberSqlArg;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct FinalMeasureSqlNode {
    input: Rc<dyn SqlNode>,
}

impl FinalMeasureSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for FinalMeasureSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(ev) => {
                /* let input = if ev.is_splitted_source() {
                    let args = visitor.evaluate_deps(node, node_processor.clone())?;
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
                } else { */
                let input = self.input.to_sql(
                    visitor,
                    node,
                    query_tools.clone(),
                    node_processor.clone(),
                )?;
                //};

                if ev.is_calculated() {
                    input
                } else {
                    let measure_type = if ev.measure_type() == "runningTotal" {
                        "sum"
                    } else {
                        &ev.measure_type()
                    };

                    format!("{}({})", measure_type, input)
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Measure filter node processor called for wrong node",
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
