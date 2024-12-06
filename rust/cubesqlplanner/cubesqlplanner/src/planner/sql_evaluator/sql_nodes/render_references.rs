use super::SqlNode;
use crate::plan::Schema;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct RenderReferencesSqlNode {
    input: Rc<dyn SqlNode>,
    schema: Rc<Schema>,
}

impl RenderReferencesSqlNode {
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
}

impl SqlNode for RenderReferencesSqlNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        let reference_column = match node.symbol() {
            MemberSymbolType::Dimension(ev) => {
                self.schema.find_column_for_member(&ev.full_name(), &None)
            }
            MemberSymbolType::Measure(ev) => {
                self.schema.find_column_for_member(&ev.full_name(), &None)
            }
            _ => None,
        };

        if let Some(reference_column) = reference_column {
            let table_ref = reference_column.table_name.as_ref().map_or_else(
                || format!(""),
                |table_name| format!("{}.", query_tools.escape_column_name(table_name)),
            );
            Ok(format!(
                "{}{}",
                table_ref,
                query_tools.escape_column_name(&reference_column.alias)
            ))
        } else {
            self.input
                .to_sql(visitor, node, query_tools.clone(), node_processor.clone())
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
