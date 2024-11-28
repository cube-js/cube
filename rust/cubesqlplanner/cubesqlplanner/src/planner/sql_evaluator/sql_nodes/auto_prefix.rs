use super::SqlNode;
use crate::plan::Schema;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbol, MemberSymbolType};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct AutoPrefixSqlNode {
    input: Rc<dyn SqlNode>,
    schema: Rc<Schema>,
}

impl AutoPrefixSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self {
            input,
            schema: Rc::new(Schema::empty()),
        })
    }

    pub fn new_with_schema(input: Rc<dyn SqlNode>, schema: Rc<Schema>) -> Rc<Self> {
        Rc::new(Self { input, schema })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }

    pub fn schema(&self) -> &Rc<Schema> {
        &self.schema
    }
}

impl SqlNode for AutoPrefixSqlNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        let input =
            self.input
                .to_sql(visitor, node, query_tools.clone(), node_processor.clone())?;
        let res = match node.symbol() {
            MemberSymbolType::Dimension(ev) => {
                let cube_alias = self.schema.resolve_cube_alias(&ev.cube_name());
                query_tools.auto_prefix_with_cube_name(&cube_alias, &input)
            }
            MemberSymbolType::Measure(ev) => {
                let cube_alias = self.schema.resolve_cube_alias(&ev.cube_name());
                query_tools.auto_prefix_with_cube_name(&cube_alias, &input)
            }
            MemberSymbolType::CubeName(_) => {
                let cube_alias = self.schema.resolve_cube_alias(&input);
                query_tools.escape_column_name(&cube_alias)
            }
            _ => input,
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
