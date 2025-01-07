use super::sql_nodes::SqlNode;
use super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct SqlEvaluatorVisitor {
    query_tools: Rc<QueryTools>,
}

impl SqlEvaluatorVisitor {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self { query_tools }
    }

    pub fn apply(
        &self,
        node: &Rc<MemberSymbol>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        let result =
            node_processor.to_sql(self, node, self.query_tools.clone(), node_processor.clone())?;
        Ok(result)
    }
}
