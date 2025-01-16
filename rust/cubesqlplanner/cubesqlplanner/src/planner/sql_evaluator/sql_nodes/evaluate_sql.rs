use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
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
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        match node.as_ref() {
            MemberSymbol::Dimension(ev) => {
                ev.evaluate_sql(visitor, node_processor.clone(), query_tools.clone())
            }
            MemberSymbol::TimeDimension(ev) => {
                let input_sql = visitor.apply(&ev.base_symbol(), node_processor.clone())?;
                let res = if let Some(granularity) = ev.granularity() {
                    let converted_tz = query_tools.base_tools().convert_tz(input_sql)?;
                    query_tools
                        .base_tools()
                        .time_grouped_column(granularity.clone(), converted_tz)?
                } else {
                    input_sql
                };
                Ok(res)
            }
            MemberSymbol::Measure(ev) => {
                ev.evaluate_sql(visitor, node_processor.clone(), query_tools.clone())
            }
            MemberSymbol::CubeTable(ev) => {
                ev.evaluate_sql(visitor, node_processor.clone(), query_tools.clone())
            }
            MemberSymbol::CubeName(ev) => ev.evaluate_sql(),
            MemberSymbol::SqlCall(s) => {
                s.eval(visitor, node_processor.clone(), query_tools.clone())
            }
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![]
    }
}
