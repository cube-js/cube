use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

pub struct LeafTimeDimensionNode {
    input: Rc<dyn SqlNode>,
    leaf_time_dimensions: HashMap<String, String>,
}

impl LeafTimeDimensionNode {
    pub fn new(input: Rc<dyn SqlNode>, leaf_time_dimensions: HashMap<String, String>) -> Rc<Self> {
        Rc::new(Self {
            input,
            leaf_time_dimensions,
        })
    }
}

impl SqlNode for LeafTimeDimensionNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        let full_name = node.full_name();
        let input_sql = self
            .input
            .to_sql(visitor, node, query_tools.clone(), node_processor)?;

        let res = if let Some(granularity) = self.leaf_time_dimensions.get(&full_name) {
            let converted_tz = query_tools.base_tools().convert_tz(input_sql)?;
            query_tools
                .base_tools()
                .time_grouped_column(granularity.clone(), converted_tz)?
        } else {
            input_sql
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
