use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct RootSqlNode {
    dimension_processor: Rc<dyn SqlNode>,
    measure_processor: Rc<dyn SqlNode>,
    cube_name_processor: Rc<dyn SqlNode>,
    default_processor: Rc<dyn SqlNode>,
}

impl RootSqlNode {
    pub fn new(
        dimension_processor: Rc<dyn SqlNode>,
        measure_processor: Rc<dyn SqlNode>,
        cube_name_processor: Rc<dyn SqlNode>,
        default_processor: Rc<dyn SqlNode>,
    ) -> Rc<Self> {
        Rc::new(Self {
            dimension_processor,
            measure_processor,
            cube_name_processor,
            default_processor,
        })
    }
}

impl SqlNode for RootSqlNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Dimension(_) => {
                self.dimension_processor
                    .to_sql(visitor, node, query_tools.clone())?
            }
            MemberSymbolType::Measure(_) => {
                self.measure_processor
                    .to_sql(visitor, node, query_tools.clone())?
            }
            MemberSymbolType::CubeName(_) => {
                self.cube_name_processor
                    .to_sql(visitor, node, query_tools.clone())?
            }
            _ => self
                .default_processor
                .to_sql(visitor, node, query_tools.clone())?,
        };
        Ok(res)
    }
}
